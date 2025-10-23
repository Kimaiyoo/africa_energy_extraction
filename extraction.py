import asyncio
import os
from playwright.async_api import async_playwright
import sys

target= "https://africa-energy-portal.org/database"

async def restart_script():
    print("Restarting script due to UI hang or download failure")
    python = sys.executable
    os.execv(python, [python] + sys.argv)

async def wait_for_loader_to_disappear(page, timeout=120000):
    """Wait for any loader to disappear."""
    try:
        print("Waiting for loader to disappear...")
        await page.wait_for_selector(
            ".loader, .ajax-loader, .spinner, .loading",
            state="hidden",
            timeout=timeout
        )
        print("Loader disappeared.")
    except Exception:
        print("No loader element detected or timeout reached, continuing anyway.")

async def select_all_filters(page):
    """Selects all filters (Year, Region, Country)"""
    print("\n--- Selecting All Filters (Year, Region, Country) ---")
    for filter_name in ["Year", "Region", "Country"]:
        try:
            dropdown_label = page.locator(f"a.custom-dropdown-label:has-text('Select {filter_name}')")
            await dropdown_label.scroll_into_view_if_needed()
            await dropdown_label.click()
            await page.wait_for_selector(
                f"input.custom-dropdown-select-all[data-name='{filter_name}']",
                timeout=15000
            )
            await page.click(f"input.custom-dropdown-select-all[data-name='{filter_name}']", force=True)
            await asyncio.sleep(1)
            await dropdown_label.click()  # close dropdown
            print(f"Selected all for {filter_name}")
        except Exception as e:
            print(f"Could not select all for {filter_name}: {e}")
            
    print("All filters selected successfully.\n")

async def process_grouping(page, grouping, is_first=False):
    folder="datasets"
    os.makedirs(folder,exist_ok=True)

    group_id = grouping.lower().replace(" ", "_")
    print(f"--- Processing {grouping} ---")

    # Switch grouping
    if not is_first:
        try:
            print(f"Switching to {grouping} ...")
            arrow = page.locator("span.select2-selection__arrow").first
            await arrow.click()
            await page.wait_for_selector("ul.select2-results__options", timeout=10000)
            option = page.locator(f"li.select2-results__option:has-text('{grouping}')")
            await option.click()
            print(f"Switched to {grouping}")
            await asyncio.sleep(3)
        except Exception as e:
            print(f"Could not switch to {grouping}: {e}")
            return

    await wait_for_loader_to_disappear(page)

    # Select all themes
    try:
        label = page.locator(f"#{group_id} label:has-text('SELECT ALL THEMES')").first
        await label.click()
        await asyncio.sleep(2)
        print(f"Selected all themes for {grouping}")
    except Exception as e:
        print(f"Could not click SELECT ALL THEMES for {grouping}: {e}")
        return

    # Click floating APPLY
    try:
        print("Waiting for floating APPLY button to appear...")
        await page.wait_for_selector("a.floating-apply-btn", timeout=20000)
        floating_apply = page.locator("a.floating-apply-btn").first
        await floating_apply.scroll_into_view_if_needed()
        await floating_apply.click()
        print(f"Clicked floating APPLY for {grouping}")
    except Exception as e:
        print(f"Floating APPLY not clickable for {grouping}: {e}")
        return

    # Wait for data to update and loader to clear
    await wait_for_loader_to_disappear(page)

    # additional wait
    await page.wait_for_timeout(10000)

    
    # Download step
    try:
        print("Initiating download...")
        download_btn = page.locator("a.download-btn.download-btn-1").first
        await download_btn.scroll_into_view_if_needed()
        #await page.wait_for_timeout(3000)
        try:
            download_btn = page.locator("a.download-btn.download-btn-1").first
            is_enabled = await download_btn.is_enabled()
            is_visible = await download_btn.is_visible()

            if not (is_enabled and is_visible):
                print(f"download button seems unresponsive for {grouping}. Restarting script")
                await restart_script()

        except:
            print(f"could not verify download state")

        

        timeout= 600000 if is_first else 300000

        async with page.expect_download(timeout=timeout) as download_info:
            await page.evaluate("""btn => btn.click()""", await download_btn.element_handle())

        download = await download_info.value
        temp_path = await download.path()
        print(f"Temporary download path: {temp_path}")
        
        filename = f"{grouping.lower().replace(' ', '_')}.xlsx"
        filepath = os.path.join(folder, filename)

        await download.save_as(filepath)
        print(f"Downloaded file saved as {filepath}")

        # check if file is downloaded
        if os.path.exists(filepath):
            print(f"File verified at {filepath}")
        else:
            print(f"File not found after save. Check browser permissions or download folder settings.")

    except Exception as e:
        print(f" Could not complete download for {grouping}: {e}")


async def main():
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=False, slow_mo=300)
        page = await browser.new_page()

        print("🌍 Navigating to Africa Energy Portal...")   
        await page.goto(target, wait_until="domcontentloaded", timeout=120000)
        await asyncio.sleep(8)

        # Initial filters (to trigger data load once)
        await select_all_filters(page)
        await wait_for_loader_to_disappear(page)

        # Process each grouping
        groupings = ["Electricity", "Energy", "Social and Economic"]
        for i, grouping in enumerate(groupings):
            await process_grouping(page, grouping, is_first=(i == 0))

        print("\n All downloads completed! Check your folder.")
        await browser.close()

if __name__ == "__main__":
    asyncio.run(main())
