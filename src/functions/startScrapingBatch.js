import logger from "loglevel";

import ScrapersTypes from "../scrapers/index.js";
import saveResult from "../utils/saveResult.js";
import { InvalidInputError } from "../utils/errors.js";

logger.setLevel("info");

export default async function startScrapingBatch(messages, browser, S3_RESULT_BUCKET_NAME) {
  const promises = messages.entries.map(
    async (entry) => {
      const response = entry;

      try {
        const {
          type,
          name,
          informations,
          createdAt,
          firstReceivedAt,
        } = entry;

        if (!type || !name) throw new InvalidInputError("Event must have type and name properties");

        const Scrapers = ScrapersTypes[type];

        if (!Scrapers) throw new InvalidInputError(`Scrapers"s type ${type} does not exists`);

        const Scraper = Scrapers[name];

        if (!Scraper) throw new InvalidInputError(`${name} is not a valid ${type} scraper`);

        const scraper = new Scraper(informations);
        const result = await scraper.scrap(browser);

        response.processingTime = Date.now() - createdAt;
        response.serviceTime = Date.now() - firstReceivedAt;
        response.resultUrl = await saveResult(result, S3_RESULT_BUCKET_NAME, { type, name });
        response.success = true;
      } catch (error) {
        logger.error(error);
        response.success = false;
      }

      return response;
    }
  );

  const results = await Promise.all(promises);

  return results;
};
