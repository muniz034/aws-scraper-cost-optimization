import logger from "loglevel";
import { workerData } from "worker_threads";

import { InvalidInputError } from "../../utils/errors.js";
import getLocalTime from "../../utils/getLocalTime.js";

export default class {
  url = 'https://servicos.coren-rj.org.br/appcorenrj/incorpnet.dll/Controller?pagina=pub_mvcLogin.htm&conselho=corenrj';

  constructor(informations) {
    const {
      registrationNumber
    } = informations;

    if (!registrationNumber) throw new InvalidInputError('Scraper must have registrationNumber information');

    this.informations = informations;
  }

  async scrap(_browser) {
    const registrationNumber = this.informations.registrationNumber;

    await page.goto(url, { waitUntil: 'networkidle2' });

    const [consultNavigateButton] = await page.$x('//button[contains(., "de Cadastro")]');

    await consultNavigateButton.click();
    
    await page.waitForNavigation({ waitUntil: 'networkidle2' });

    await page.type('input[name="EDT_NumeroInscricao"]', registrationNumber);

    await page.click('input[name="BTN_Consultar"]');

    await page.waitForSelector('td');

    const html = await page.content();
    
    logger.info(getLocalTime(), `[${workerData.id}] Fetched response`);

    await page.close();

    return html;
  }
};