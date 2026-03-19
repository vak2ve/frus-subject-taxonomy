import { readdirSync } from 'fs';
import { resolve } from 'path';
import { createRequire } from 'module';

// Use globally installed playwright which has matching browsers
const { chromium } = await import('/opt/node22/lib/node_modules/playwright/index.js');

const dir = resolve(import.meta.dirname);
const htmlFiles = readdirSync(dir).filter(f => f.endsWith('.html'));

const browser = await chromium.launch();

for (const file of htmlFiles) {
  const page = await browser.newPage();
  const filePath = resolve(dir, file);
  const pngName = file.replace('.html', '.png');

  await page.setViewportSize({ width: 1280, height: 900 });
  await page.goto(`file://${filePath}`, { waitUntil: 'networkidle' });
  // Small delay for font loading
  await page.waitForTimeout(500);
  await page.screenshot({ path: resolve(dir, pngName), fullPage: false });
  await page.close();
  console.log(`✓ ${pngName}`);
}

await browser.close();
console.log(`\nDone! ${htmlFiles.length} screenshots saved.`);
