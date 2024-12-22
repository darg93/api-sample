const { queue } = require('async');
const _ = require('lodash');

const hubspotClient = require('./services/hubspot/hubspot-client');
const companyService = require('./services/hubspot/company-service');
const contactService = require('./services/hubspot/contact-service');
const meetingService = require('./services/hubspot/meeting-service');
const Domain = require('./models/domain-model');
const logger = require('./logger/logger');
const { goal } = require('./utils/goal.utils');

class HubspotWorker {
  constructor() {
    this.BATCH_SIZE = 2000;
  }

  createQueue(domain, actions) {
    return queue(async (action, callback) => {
      actions.push(action);

      if (actions.length > this.BATCH_SIZE) {
        logger.info('Inserting actions to database', {
          apiKey: domain.apiKey,
          count: actions.length
        });

        const copyOfActions = _.cloneDeep(actions);
        actions.splice(0, actions.length);
        await goal(copyOfActions);
      }

      callback();
    }, 100000000);
  }

  async drainQueue(domain, actions, q) {
    if (q.length() > 0) {
      await q.drain();
    }

    if (actions.length > 0) {
      await goal(actions);
    }

    return true;
  }

  async saveDomain(domain) {
    // Disabled for testing
    return;
    domain.markModified('integrations.hubspot.accounts');
    await domain.save();
  }

  async processAccount(domain, account) {
    logger.info('Processing HubSpot account', { 
      hubId: account.hubId 
    });

    try {
      await hubspotClient.refreshAccessToken(domain, account.hubId);
    } catch (error) {
      logger.error('Error refreshing access token', {
        error,
        hubId: account.hubId,
        domainId: domain._id
      });
      throw error;
    }

    const actions = [];
    const q = this.createQueue(domain, actions);

    try {
      await contactService.fetchContacts(domain, account.hubId, q);
      logger.info('Contacts processed successfully', {
        hubId: account.hubId
      });
    } catch (error) {
      logger.error('Error processing contacts', {
        error,
        hubId: account.hubId,
        domainId: domain._id
      });
    }

    try {
      await companyService.fetchCompanies(domain, account.hubId, q);
      logger.info('Companies processed successfully', {
        hubId: account.hubId
      });
    } catch (error) {
      logger.error('Error processing companies', {
        error,
        hubId: account.hubId,
        domainId: domain._id
      }); 
    }

    try {
      await meetingService.fetchMeetings(domain, account.hubId, q);
      logger.info('Meetings processed successfully', {
        hubId: account.hubId
      });
    } catch (error) {
      logger.error('Error processing Meetings', {
        error,
        hubId: account.hubId,
        domainId: domain._id
      });
    }

    try {
      await this.drainQueue(domain, actions, q);
      logger.info('Queue drained successfully', {
        hubId: account.hubId
      });
    } catch (error) {
      logger.error('Error draining queue', {
        error,
        hubId: account.hubId,
        domainId: domain._id
      });
    }

    await this.saveDomain(domain);
    logger.info('Account processing completed', {
      hubId: account.hubId
    });
  }

  async pullDataFromHubspot() {
    logger.info('Starting HubSpot data pull');

    try {
      const domain = await Domain.findOne({});

      for (const account of domain.integrations.hubspot.accounts) {
        await this.processAccount(domain, account);
      }

      logger.info('HubSpot data pull completed successfully');
    } catch (error) {
      logger.error('Error pulling data from HubSpot', { error });
      throw error;
    }

    process.exit();
  }
}

module.exports = new HubspotWorker();
