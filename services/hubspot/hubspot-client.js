const hubspot = require('@hubspot/api-client');
const logger = require('../../logger/logger');

class HubspotClient {
  constructor(config) {
    this.client = new hubspot.Client({ accessToken: '' });
    this.config = config;
    this.expirationDate = null;
  }

  async refreshAccessToken(domain, hubId) {
    try {
      const account = domain.integrations.hubspot.accounts.find(acc => acc.hubId === hubId);
      const { accessToken, refreshToken } = account;
      const { HUBSPOT_CID, HUBSPOT_CS } = process.env;

      const result = await this.client.oauth.tokensApi.createToken(
        'refresh_token',
        undefined,
        undefined,
        HUBSPOT_CID,
        HUBSPOT_CS,
        refreshToken
      );

      const body = result.body || result;
      const newAccessToken = body.accessToken;
      this.expirationDate = new Date(body.expiresIn * 1000 + Date.now());

      this.client.setAccessToken(newAccessToken);

      if (newAccessToken !== accessToken) {
        account.accessToken = newAccessToken;
      }

      return true;
    } catch (error) {
      logger.error('Error refreshing access token', {
        error,
        hubId,
        domainId: domain._id
      });
      throw error;
    }
  }

  async executeWithRetry(operation, domain, hubId, maxRetries = 4) {
    let retryCount = 0;
    
    while (retryCount <= maxRetries) {
      try {
        return await operation();
      } catch (error) {
        retryCount++;
        
        if (Date.now() > this.expirationDate) {
          await this.refreshAccessToken(domain, hubId);
        }

        if (retryCount === maxRetries) {
          throw error;
        }

        await new Promise(resolve => 
          setTimeout(resolve, 5000 * Math.pow(2, retryCount))
        );
      }
    }
  }

  getClient() {
    return this.client;
  }
}

module.exports = new HubspotClient();
