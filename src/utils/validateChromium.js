import { BrowserFetcher } from "puppeteer";
import getLocalTime from "./getLocalTime.js";
import logger from "loglevel";
import path from "path";
import os from "os";

export default async function validateChromium() {
  let browserFetcher = new BrowserFetcher({path: path.join(os.homedir(), ".cache", "puppeteer")});

  const chromiumRevisionInfo = browserFetcher.revisionInfo("1069273");
  const isChromiumDownloaded = chromiumRevisionInfo.local;

  if(!isChromiumDownloaded){
    logger.info(getLocalTime(), `Chromium is not installed, installing revision ${chromiumRevisionInfo.revision} in ${chromiumRevisionInfo.folderPath}`);
    
    const canDownload = await browserFetcher.canDownload(chromiumRevisionInfo.revision);
    
    if(canDownload){
      logger.info(getLocalTime(), `Downloading Chromium...`);
      await browserFetcher.download(chromiumRevisionInfo.revision);
      logger.info(getLocalTime(), `Downloading complete`);
      return true;
    } else {
      logger.info(getLocalTime(), `It was not possible to download Chromium`);
      return false;
    }
  }
}