import chromium from "chrome-aws-lambda";
import puppeteer from "puppeteer";
import config from "config";

import { ConfigurationError } from "./errors.js";

// In order to mimic the flags utilized in chrome-aws-lambda
const ChromiumArgs = [
  '--allow-running-insecure-content', // https://source.chromium.org/search?q=lang:cpp+symbol:kAllowRunningInsecureContent&ss=chromium
  '--autoplay-policy=user-gesture-required', // https://source.chromium.org/search?q=lang:cpp+symbol:kAutoplayPolicy&ss=chromium
  '--disable-component-update', // https://source.chromium.org/search?q=lang:cpp+symbol:kDisableComponentUpdate&ss=chromium
  '--disable-domain-reliability', // https://source.chromium.org/search?q=lang:cpp+symbol:kDisableDomainReliability&ss=chromium
  '--disable-features=AudioServiceOutOfProcess,IsolateOrigins,site-per-process', // https://source.chromium.org/search?q=file:content_features.cc&ss=chromium
  '--disable-print-preview', // https://source.chromium.org/search?q=lang:cpp+symbol:kDisablePrintPreview&ss=chromium
  '--disable-setuid-sandbox', // https://source.chromium.org/search?q=lang:cpp+symbol:kDisableSetuidSandbox&ss=chromium
  '--disable-site-isolation-trials', // https://source.chromium.org/search?q=lang:cpp+symbol:kDisableSiteIsolation&ss=chromium
  '--disable-speech-api', // https://source.chromium.org/search?q=lang:cpp+symbol:kDisableSpeechAPI&ss=chromium
  '--disable-web-security', // https://source.chromium.org/search?q=lang:cpp+symbol:kDisableWebSecurity&ss=chromium
  '--disk-cache-size=33554432', // https://source.chromium.org/search?q=lang:cpp+symbol:kDiskCacheSize&ss=chromium
  '--enable-features=SharedArrayBuffer', // https://source.chromium.org/search?q=file:content_features.cc&ss=chromium
  '--hide-scrollbars', // https://source.chromium.org/search?q=lang:cpp+symbol:kHideScrollbars&ss=chromium
  '--ignore-gpu-blocklist', // https://source.chromium.org/search?q=lang:cpp+symbol:kIgnoreGpuBlocklist&ss=chromium
  '--in-process-gpu', // https://source.chromium.org/search?q=lang:cpp+symbol:kInProcessGPU&ss=chromium
  '--mute-audio', // https://source.chromium.org/search?q=lang:cpp+symbol:kMuteAudio&ss=chromium
  '--no-default-browser-check', // https://source.chromium.org/search?q=lang:cpp+symbol:kNoDefaultBrowserCheck&ss=chromium
  '--no-pings', // https://source.chromium.org/search?q=lang:cpp+symbol:kNoPings&ss=chromium
  '--no-sandbox', // https://source.chromium.org/search?q=lang:cpp+symbol:kNoSandbox&ss=chromium
  '--no-zygote', // https://source.chromium.org/search?q=lang:cpp+symbol:kNoZygote&ss=chromium
  '--use-gl=swiftshader', // https://source.chromium.org/search?q=lang:cpp+symbol:kUseGl&ss=chromium
  '--window-size=1920,1080', // https://source.chromium.org/search?q=lang:cpp+symbol:kWindowSize&ss=chromium
  '--single-process' // Para utilizar as threads de forma que não tenha um overhead muito grande!
];

export default async function launchBrowser () {
  let executablePath;
  
  return puppeteer.launch({
    args: ChromiumArgs,
    headless: true,
  });

  // if(config.get("Enviroment").IS_INSTANCE){
  //   const { CHROME_PATH: path } = process.env;

  //   if (!path) throw new ConfigurationError("Must export CHROME_PATH environment variable");

  //   executablePath = process.env.CHROME_PATH;

  //   return chromium.puppeteer.launch({
  //     args: chromium.args,
  //     headless: true,
  //     executablePath,
  //   });
  // } else if(config.get("Enviroment").IS_LOCAL) {
  //   return puppeteer.launch({
  //     args: ChromiumArgs,
  //     headless: true,
  //   });
  // } else {
  //   executablePath = await chromium.executablePath;
  //   return chromium.puppeteer.launch({
  //     args: chromium.args,
  //     headless: true,
  //     executablePath,
  //   });
  // }
};
