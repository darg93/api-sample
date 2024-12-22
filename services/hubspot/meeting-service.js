const hubspotClient = require('./hubspot-client');
const { generateLastModifiedDateFilter } = require('../../utils/filtering.utils');
const logger = require('../../logger/logger');

class MeetingService {
  constructor() {
    this.client = hubspotClient.getClient();
    this.BATCH_SIZE = 100;
  }

  async searchMeetings(searchObject) {   
    return await this.client.crm.objects.meetings.searchApi.doSearch(searchObject);
  }

  async fetchContactsForMeetings(meetingIds) {
    try {
      const attendeesResults = await this.client.apiRequest({
        method: 'post',
        path: '/crm/v3/associations/MEETINGS/CONTACTS/batch/read',
        body: { inputs: meetingIds.map(meetingId => ({ id: meetingId })) }
      });
      return (await attendeesResults.json())?.results || [];
    } catch (error) {
      logger.error('Error fetching meeting attendees', { error });
      return [];
    }
  }

  async fetchContactDetails(contactIds) {
    const contactDetailsPromises = contactIds.map(async (contactId) => {
      try {
        const contact = await this.client.crm.contacts.basicApi.getById(
          contactId, 
          ['email']
        );
        return { 
          contactId, 
          email: contact.properties.email 
        };
      } catch (error) {
        logger.error('Failed to fetch contact details', { 
          error, 
          contactId 
        });
        return null;
      }
    });

    const results = await Promise.all(contactDetailsPromises);
    return results.filter(detail => detail !== null);
  }

  async processMeetingBatch(meetings, attendeesMap, contactDetailsMap, lastPulledDate, queue) {
    meetings.forEach(meeting => {
      if (!meeting.properties) return;

      const contactId = attendeesMap[meeting.id];
      const contactEmail = contactId ? contactDetailsMap[contactId] : null;

      const meetingProperties = {
        meeting_id: meeting.id,
        meeting_title: meeting.properties.hs_meeting_title,
        meeting_start_time: meeting.properties.hs_meeting_start_time 
          ? new Date(parseInt(meeting.properties.hs_meeting_start_time)).toISOString() 
          : null,
        meeting_end_time: meeting.properties.hs_meeting_end_time 
          ? new Date(parseInt(meeting.properties.hs_meeting_end_time)).toISOString() 
          : null,
        meeting_outcome: meeting.properties.hs_meeting_outcome,
        contact_id: contactId
      };

      const isCreated = !lastPulledDate || 
        (new Date(meeting.createdAt) > lastPulledDate);

      const actionTemplate = {
        includeInAnalytics: 0,
        meetingProperties: meetingProperties,
        ...(contactEmail ? { identity: contactEmail } : {})
      };

      queue.push({
        actionName: isCreated ? 'Meeting Created' : 'Meeting Updated',
        actionDate: new Date(isCreated ? meeting.createdAt : meeting.updatedAt),
        ...actionTemplate
      });
    });
  }

  async fetchMeetings(domain, hubId, queue) {
    try {
      const account = domain.integrations.hubspot.accounts
        .find(acc => acc.hubId === hubId);

      // Initialize meetings last pulled date if not exists
      if (!account.lastPulledDates?.meetings) {
        account.lastPulledDates = account.lastPulledDates || {};
        account.lastPulledDates.meetings = new Date('2000-01-01');
      }

      const lastPulledDate = new Date(account.lastPulledDates.meetings);
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
            'hs_meeting_title',
            'hs_meeting_start_time',
            'hs_meeting_end_time',
            'hs_meeting_outcome'
          ],
          limit: this.BATCH_SIZE,
          after: offsetObject.after
        };

        const searchResult = await hubspotClient.executeWithRetry(
          () => this.searchMeetings(searchObject),
          domain,
          hubId
        );

        const meetings = searchResult?.results || [];
        logger.info('Processing meeting batch', { 
          count: meetings.length,
          hubId 
        });

        if (meetings.length > 0) {
          // Fetch attendees for meetings
          const meetingIds = meetings.map(meeting => meeting.id);
          const attendeesResults = await this.fetchContactsForMeetings(meetingIds);

          // Create meetings to contacts map
          const attendeesMap = {};
          attendeesResults.forEach(result => {
            if (result.from && result.to && result.to.length > 0) {
              attendeesMap[result.from.id] = result.to[0].id;
            }
          });

          // Fetch contact details
          const uniqueContactIds = [...new Set(Object.values(attendeesMap))];
          const contactDetails = await this.fetchContactDetails(uniqueContactIds);
          const contactDetailsMap = Object.fromEntries(
            contactDetails.map(detail => [detail.contactId, detail.email])
          );

          await this.processMeetingBatch(
            meetings, 
            attendeesMap, 
            contactDetailsMap, 
            lastPulledDate, 
            queue
          );
        }

        offsetObject.after = parseInt(searchResult?.paging?.next?.after);

        if (!offsetObject?.after) {
          hasMore = false;
          break;
        } else if (offsetObject?.after >= 9900) {
          offsetObject.after = 0;
          offsetObject.lastModifiedDate = new Date(
            meetings[meetings.length - 1].updatedAt
          ).valueOf();
        }
      }

      account.lastPulledDates.meetings = now;
      return true;

    } catch (error) {
      logger.error('Error processing meetings', {
        error,
        hubId,
        domainId: domain._id
      });
      throw error;
    }
  }
}

module.exports = new MeetingService();
