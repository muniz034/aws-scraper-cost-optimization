import logger from "loglevel";
import { workerData } from "worker_threads";

// import launchBrowser from "../../utils/launchBrowser.js";
import { InvalidInputError } from "../../utils/errors.js";
import getLocalTime from "../../utils/getLocalTime.js";

export default class {
  url = "https://esaj.tjsp.jus.br/cposg/open.do";

  constructor(informations) {
    const {
      cpf,
    } = informations;

    if (!cpf) throw new InvalidInputError("Scraper must have cpf information");

    this.informations = informations;
  }

  async scrap(_browser) {
    if(!_browser) throw new Error("Expecting a browser instance to be running to start Scraping");

    const browser = _browser;

    const cpf = this.informations.cpf;

    const page = await browser.newPage();

    await page.goto(this.url, {timeout: 0});

    await page.select("select[name='cbPesquisa']", "DOCPARTE");

    await page.type("input[id='campo_DOCPARTE']", cpf);

    await Promise.all([
      await page.click("input[type='submit']"),
      await page.waitForSelector("div[id='listagemDeProcessos']")
    ]);

    const html = await page.content();

    logger.info(getLocalTime(), `[${workerData.id}] Fetched response`);

    await page.close();

    return html;
  }
};