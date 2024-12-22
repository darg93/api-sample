const hubspotClient = require('./hubspot-client');
const { generateLastModifiedDateFilter } = require('../../utils/filtering.utils');
const logger = require('../../logger/logger');

class CompanyService {
  constructor() {
    this.client = hubspotClient.getClient();
    this.BATCH_SIZE = 100;
  }

  async searchCompanies(searchObject) {
    return await this.client.crm.companies.searchApi.doSearch(searchObject);
  }

  async processCompanyBatch(companies, lastPulledDate, queue) {
    companies.forEach(company => {
      if (!company.properties) return;

      const actionTemplate = {
        includeInAnalytics: 0,
        companyProperties: {
          company_id: company.id,
          company_domain: company.properties.domain,
          company_industry: company.properties.industry
        }
      };

      const isCreated = !lastPulledDate || 
        (new Date(company.createdAt) > lastPulledDate);

      queue.push({
        actionName: isCreated ? 'Company Created' : 'Company Updated',
        actionDate: new Date(isCreated ? company.createdAt : company.updatedAt) - 2000,
        ...actionTemplate
      });
    });
  }

  async fetchCompanies(domain, hubId, queue) {
    try {
      const account = domain.integrations.hubspot.accounts
        .find(acc => acc.hubId === hubId);
      const lastPulledDate = new Date(account.lastPulledDates.companies);
      const now = new Date();

      let hasMore = true;
      const offsetObject = {};

      while (hasMore) {
        const lastModifiedDate = offsetObject.lastModifiedDate || lastPulledDate;
        const searchObject = {
          filterGroups: [
            generateLastModifiedDateFilter(lastModifiedDate, now)
          ],
          sorts: [
            { 
              propertyName: 'hs_lastmodifieddate', 
              direction: 'ASCENDING' 
            }
          ],
          properties: [
            'name',
            'domain',
            'country',
            'industry',
            'description',
            'annualrevenue',
            'numberofemployees',
            'hs_lead_status'
          ],
          limit: this.BATCH_SIZE,
          after: offsetObject.after
        };

        const searchResult = await hubspotClient.executeWithRetry(
          () => this.searchCompanies(searchObject),
          domain,
          hubId
        );

        const companies = searchResult?.results || [];
        logger.info('Processing company batch', { 
          count: companies.length,
          hubId 
        });

        await this.processCompanyBatch(companies, lastPulledDate, queue);

        offsetObject.after = parseInt(searchResult?.paging?.next?.after);

        if (!offsetObject?.after) {
          hasMore = false;
          break;
        } else if (offsetObject?.after >= 9900) {
          offsetObject.after = 0;
          offsetObject.lastModifiedDate = new Date(
            companies[companies.length - 1].updatedAt
          ).valueOf();
        }
      }

      account.lastPulledDates.companies = now;
      return true;

    } catch (error) {
      logger.error('Error processing companies', {
        error,
        hubId,
        domainId: domain._id
      });
      throw error;
    }
  }
}

module.exports = new CompanyService();
