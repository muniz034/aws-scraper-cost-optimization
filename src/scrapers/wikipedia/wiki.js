import logger from "loglevel";
import { workerData } from "worker_threads";

import { InvalidInputError } from "../../utils/errors.js";
import getLocalTime from "../../utils/getLocalTime.js";

export default class {
  url = "https://en.wikipedia.org/wiki/Main_Page";

  constructor(informations) {
    const {
      search,
    } = informations;

    if (!search) throw new InvalidInputError("Scraper must have search information");

    this.informations = informations;
  }

  async scrap(_browser) {
    if(!_browser) throw new Error("Expecting a browser instance to be running to start Scraping");

    const browser = _browser;

    const search = this.informations.search;

    const page = await browser.newPage();

    await page.goto(this.url, {timeout: 0});

    await page.type("input[id='searchInput']", search);

    await page.click("input[id='searchButton']");

    await page.waitForFunction(() => document.readyState === "complete");

    const html = await page.content();

    logger.info(getLocalTime(), `[${workerData.id}] Fetched response`);

    await page.close();

    return html;
  }
};