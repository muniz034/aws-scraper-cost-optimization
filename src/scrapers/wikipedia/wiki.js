import logger from "loglevel";
import { workerData } from "worker_threads";

import { InvalidInputError } from "../../utils/errors.js";
import getLocalTime from "../../utils/getLocalTime.js";
import sleep from "../../utils/sleep.js";

export default class {
  url = "https://en.wikipedia.org/w/index.php?search=&title=Special:Search";

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

    await page.type("input[type='search']", search);

    await page.click("button.oo-ui-inputWidget-input");

    await page.waitForFunction(() => document.readyState === "complete");

    await page.click("a[data-serp-pos='0']");

    await page.waitForFunction(() => document.readyState === "complete");

    const html = await page.content();

    logger.info(getLocalTime(), `[${workerData ? workerData.id : "MASTER"}] Fetched response`);

    await page.close();

    return html;
  }
};