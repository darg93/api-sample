const hubspotClient = require('./hubspot-client');
const { generateLastModifiedDateFilter } = require('../../utils/filtering.utils');
const { filterNullValuesFromObject } = require('../../utils/filtering.utils');
const logger = require('../../logger/logger');

class ContactService {
  constructor() {
    this.client = hubspotClient.getClient();
    this.BATCH_SIZE = 100;
  }

  async searchContacts(searchObject) {
    return await this.client.crm.contacts.searchApi.doSearch(searchObject);
  }

  async getCompanyAssociations(contactIds) {
    const result = await this.client.apiRequest({
      method: 'post',
      path: '/crm/v3/associations/CONTACTS/COMPANIES/batch/read',
      body: { 
        inputs: contactIds.map(contactId => ({ id: contactId })) 
      }
    });
    return (await result.json())?.results || [];
  }

  async processContactBatch(contacts, companyAssociations, lastPulledDate, queue) {
    contacts.forEach(contact => {
      if (!contact.properties?.email) return;

      const companyId = companyAssociations[contact.id];
      const isCreated = new Date(contact.createdAt) > lastPulledDate;

      const userProperties = {
        company_id: companyId,
        contact_name: `${contact.properties.firstname || ''} ${contact.properties.lastname || ''}`.trim(),
        contact_title: contact.properties.jobtitle,
        contact_source: contact.properties.hs_analytics_source,
        contact_status: contact.properties.hs_lead_status,
        contact_score: parseInt(contact.properties.hubspotscore) || 0
      };

      const actionTemplate = {
        includeInAnalytics: 0,
        identity: contact.properties.email,
        userProperties: filterNullValuesFromObject(userProperties)
      };

      queue.push({
        actionName: isCreated ? 'Contact Created' : 'Contact Updated',
        actionDate: new Date(isCreated ? contact.createdAt : contact.updatedAt),
        ...actionTemplate
      });
    });
  }

  async fetchContacts(domain, hubId, queue) {
    try {
      const account = domain.integrations.hubspot.accounts
        .find(acc => acc.hubId === hubId);
      const lastPulledDate = new Date(account.lastPulledDates.contacts);
      const now = new Date();

      let hasMore = true;
      const offsetObject = {};

      while (hasMore) {
        const lastModifiedDate = offsetObject.lastModifiedDate || lastPulledDate;
        const searchObject = {
          filterGroups: [
            generateLastModifiedDateFilter(lastModifiedDate, now, 'lastmodifieddate')
          ],
          sorts: [
            { 
              propertyName: 'lastmodifieddate', 
              direction: 'ASCENDING' 
            }
          ],
          properties: [
            'firstname',
            'lastname',
            'jobtitle',
            'email',
            'hubspotscore', 
            'hs_lead_status',
            'hs_analytics_source',
            'hs_latest_source'
          ],
          limit: this.BATCH_SIZE,
          after: offsetObject.after
        };

        const searchResult = await hubspotClient.executeWithRetry(
          () => this.searchContacts(searchObject),
          domain,
          hubId
        );

        const contacts = searchResult.results || [];
        logger.info('Processing contact batch', { 
          count: contacts.length,
          hubId 
        });

        const contactIds = contacts.map(contact => contact.id);
        const associations = await this.getCompanyAssociations(contactIds);
        
        const companyAssociations = Object.fromEntries(
          associations
            .map(a => {
              if (a.from) {
                return [a.from.id, a.to[0].id];
              }
              return false;
            })
            .filter(Boolean)
        );

        await this.processContactBatch(
          contacts, 
          companyAssociations,
          lastPulledDate,
          queue
        );

        offsetObject.after = parseInt(searchResult.paging?.next?.after);

        if (!offsetObject?.after) {
          hasMore = false;
          break;
        } else if (offsetObject?.after >= 9900) {
          offsetObject.after = 0;
          offsetObject.lastModifiedDate = new Date(
            contacts[contacts.length - 1].updatedAt
          ).valueOf();
        }
      }

      account.lastPulledDates.contacts = now;
      return true;

    } catch (error) {
      logger.error('Error processing contacts', {
        error,
        hubId,
        domainId: domain._id
      });
      throw error;
    }
  }
}

module.exports = new ContactService();
