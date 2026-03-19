const { chromium } = require('/opt/node22/lib/node_modules/playwright');
const { readdirSync } = require('fs');
const { resolve } = require('path');

(async () => {
  const dir = __dirname;
  const htmlFiles = readdirSync(dir).filter(f => f.endsWith('.html'));

  const browser = await chromium.launch();

  for (const file of htmlFiles) {
    const page = await browser.newPage();
    const filePath = resolve(dir, file);
    const pngName = file.replace('.html', '.png');

    await page.setViewportSize({ width: 1280, height: 900 });
    await page.goto(`file://${filePath}`, { waitUntil: 'domcontentloaded', timeout: 10000 });
    await page.waitForTimeout(300);
    await page.screenshot({ path: resolve(dir, pngName), fullPage: false });
    await page.close();
    console.log(`✓ ${pngName}`);
  }

  await browser.close();
  console.log(`\nDone! ${htmlFiles.length} screenshots saved.`);
})();
