import asyncio
import aiohttp
import os
from tqdm import tqdm
from bs4 import BeautifulSoup
from aiohttp.client_exceptions import ClientConnectorError

# Constants
RATE_LIMIT = 5  # Number of requests per second
OUTPUT_DIR = "output"
MAX_TITLE_LENGTH = 100
written_urls = set()

semaphore = asyncio.Semaphore(RATE_LIMIT)

async def fetch_url(session, url):
    async with semaphore:
        try:
            async with session.get(url, timeout=15) as response:
                content = await response.text()
                soup = BeautifulSoup(content, 'html.parser')
                title = soup.title.string if soup.title else "No title"
                title = title.replace('\n', ' ').replace('\r', '').strip()  # Remove newline characters
                title = title[:MAX_TITLE_LENGTH] + '...' if len(title) > MAX_TITLE_LENGTH else title  # Truncate if too long

                if url in written_urls:
                    return  # Skip if URL has already been written to the output files

                if 400 <= response.status < 600:
                    with open(os.path.join(OUTPUT_DIR, "discarded_links.txt"), "a", encoding="utf-8") as file:
                        file.write(f"{url}, Status: {response.status}\n")  # Add status to discarded items
                    written_urls.add(url)
                    return (url, str(response.status), None)
                
                with open(os.path.join(OUTPUT_DIR, "useful_links.txt"), "a", encoding="utf-8") as file:
                    file.write(f"{url},{title}\n")  # CSV format
                written_urls.add(url)
                return (url, str(response.status), title)
        except (aiohttp.ClientError, ClientConnectorError, asyncio.TimeoutError) as e:
            if url not in written_urls:
                with open(os.path.join(OUTPUT_DIR, "discarded_links.txt"), "a", encoding="utf-8") as file:
                    file.write(f"{url}, Error: {str(e)}\n")  # Add error reason to discarded items
                written_urls.add(url)
            return (url, 'Error', str(e))
        finally:
            with open(os.path.join(OUTPUT_DIR, "processed_urls.txt"), "a", encoding="utf-8") as file:
                file.write(url + "\n")


async def main_async():
    print(f"Total URLs to check: {len(urls)}")
    print(f"URLs already processed: {len(processed_urls)}")
    print(f"URLs to be processed in this run: {len(urls) - len(processed_urls)}")

    async with aiohttp.ClientSession() as session:
        tasks = [fetch_url(session, url) for url in urls if url not in processed_urls]
        for _ in tqdm(asyncio.as_completed(tasks), total=len(urls) - len(processed_urls), desc="Checking URLs", unit="URL"):
            try:
                await _
            except Exception as e:
                print(f"Error while processing a URL: {e}")


if __name__ == "__main__":
    with open("input.txt", "r") as file:
        urls = [line.strip() for line in file]

    # Create output directory if it doesn't exist
    if not os.path.exists(OUTPUT_DIR):
        os.makedirs(OUTPUT_DIR)

    # Load processed URLs
    try:
        with open(os.path.join(OUTPUT_DIR, "processed_urls.txt"), "r", encoding="utf-8") as file:
            processed_urls = set(line.strip() for line in file)
    except FileNotFoundError:
        processed_urls = set()

    asyncio.run(main_async())

    print(f"Finished processing!")
