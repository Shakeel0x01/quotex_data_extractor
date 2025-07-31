from playwright.async_api import async_playwright
from playwright_stealth import stealth_async
from dotenv import load_dotenv
from lxml import html
import aioimaplib
import asyncio
import random
import email
import json
import re
import os

load_dotenv()
email = os.environ['email']
password = os.environ['password']
app_password = os.environ['app_password']

async def type_human_like(page, selector, text, delay_range=(0.1, 0.3)):
    for char in text:
        await page.type(selector, char)
        await asyncio.sleep(random.uniform(*delay_range))

async def _extract_session_data_stealth(qx_email, qx_password, email_app_pass):
    async with async_playwright() as playwright:
        browser = await playwright.firefox.launch(headless=False)
        context = await browser.new_context(
            viewport={"width": 1920, "height": 1080},
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:119.0) Gecko/20100101 Firefox/119.0",
        )
        page = await context.new_page()

        # make it stealth
        await stealth_async(context)

        # Connect to the IMAP server
        client = aioimaplib.IMAP4_SSL('imap.gmail.com')
        await client.wait_hello_from_server()

        # login
        rslt = await client.login(qx_email, email_app_pass)
        if rslt.result != "OK":
            return None

        # Search the mails from Quotex
        await client.select('INBOX')
        status, messages = await client.search(f'(FROM "{"noreply@qxbroker.com"}")')

        # Fetch the last mail's id
        messages = messages[0].decode().split()
        last_mail_id = messages[-1] if messages else None

        # Navigate to the login page
        await page.goto("https://qxbroker.com/sign-in/modal/")
        await page.wait_for_load_state("networkidle")

        # Fill login form
        await page.fill('input[type="email"]', qx_email)
        await page.fill('input[type="password"]', qx_password)
        await page.click('button[type="submit"]')
        await page.wait_for_timeout(2000)  

        # Handle 2FA (if required)
        tree = html.fromstring(await page.content())
        if tree.xpath('//input[@name="keep_code"]'):
            # wait for the email to arrive
            bFinished = 0
            while not bFinished:
                await client.select('INBOX')
                status, messages = await client.search(f'(FROM "{"noreply@qxbroker.com"}")')
                
                if not messages: continue

                messages = messages[0].decode().split()
                strt_idx = 0 if not last_mail_id else messages.index(last_mail_id) + 1
                if strt_idx >= len(messages): continue # No new mail yet
                messages = messages[strt_idx:]
                last_mail_id = messages[-1]

                for id_ in messages:
                    mail = await client.fetch(id_, '(RFC822)')
                    code = parse_code_from_raw_message(mail.lines[1])

                    if not code or code == -1: continue

                    if code:
                        await type_human_like(page, 'input[name="code"]', code)
                        await page.click('button[type="submit"]')
                        await page.wait_for_load_state("domcontentloaded")

                        if "trade" in page.url:
                            bFinished = 1
                            break

        # Extract session data
        source = await page.content()
        await browser.close()
        await client.logout()

        return extract_token_from_source_qx(source)
    
async def extract_session_data_stealth(qx_email, qx_password, email_app_pass, timeout=60):
    try:
        return await asyncio.wait_for(
            _extract_session_data_stealth(qx_email, qx_password, email_app_pass), 
            timeout=timeout
        )
    except asyncio.TimeoutError:
        return -1

def extract_token_from_source_qx(src):
    for tag in html.fromstring(src).xpath('//script[@type="text/javascript"]'):
        if "window.settings" in tag.text:
            settings = tag.text.strip().replace(";", "")
            return json.loads(settings.split("window.settings = ", 1)[-1]).get("token")
    return None

def parse_code_from_raw_message(raw_msg):
    msg = email.message_from_bytes(raw_msg)

    # Check if the message is multipart. Will implement this later if needed
    # Quotex email is single part every time
    if msg.is_multipart(): return -1
    msg['Subject'] == "Authentication pincode"
    body = msg.get_payload(decode=True).decode()
    match = re.search(r'<b>(\d+)</b>', body)
    if match:
        return match.group(1)
    return None

# enter credentials
print(asyncio.run(extract_session_data_stealth(email, password, app_password)))
