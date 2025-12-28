import urllib.request
import xml.etree.ElementTree as ET
import boto3
import os
import time
import json
from datetime import datetime, timezone

# --- CONFIGURATION RSS FEED REDDIT ---
RSS_URL = "https://www.reddit.com/r/LeaseTakeoverNYC+NYCapartments/search.rss?q=%28%222BR%22+OR+%22Two+bed%22+OR+%22Two+bedrooms%22+OR+%222B%22+OR+%22Two+B%22%29&restrict_sr=1&sort=new"

# Using Google Gemma 3 12B 
BEDROCK_MODEL_ID = "google.gemma-3-12b-it"

# XML Namespace for Atom Feeds 
NAMESPACES = {'atom': 'http://www.w3.org/2005/Atom'}

sns = boto3.client('sns')
dynamodb = boto3.resource('dynamodb')
bedrock = boto3.client('bedrock-runtime')

SNS_TOPIC_ARN = os.environ.get('SNS_TOPIC_ARN') 
TABLE_NAME = os.environ.get('TABLE_NAME') 

def get_seen_posts(table):
    """Retrieves posts already seen from the DB."""
    response = table.scan()
    items = response.get('Items', [])
    print(f"[DB] Retrieved {len(items)} previously seen posts from database.")
    return {item['post_id'] for item in items}

def save_post_id(table, post_id, title):
    """Saves a post in the DB as 'seen' with a 14-day expiration."""
    ttl = int(time.time()) + (14 * 24 * 60 * 60) 
    found_at = datetime.now(timezone.utc).isoformat()
    table.put_item(Item={
        'post_id': post_id, 
        'title': title, 
        'found_at': found_at, 
        'ttl': ttl
    })

def ask_bedrock_analysis(title, body):
    """
    Asks Google Gemma if the apartment meets the criteria.
    Returns True ("SEND") or False ("SKIP").
    """
    
    prompt_text = f"""
    You are an AI Real Estate Agent filtering NYC apartments.
    
    TASK: Analyze this listing. Reply ONLY with "SEND" or "SKIP".
    
    CRITERIA (All must be true for SEND):
    1. It is a 2-Bedroom apartment (2BR, 2 Bed).
    2. ENTIRE unit only (NO roommates, NO single rooms).
    3. NOT asking for advice/feedback.
    4. Location is NOT: Brooklyn, NJ, LIC, or Roosevelt Island.
    
    LISTING:
    Title: {title}
    Body: {body}
    
    RESPONSE:
    """

    try:
        response = bedrock.converse(
            modelId=BEDROCK_MODEL_ID,
            messages=[{
                "role": "user",
                "content": [{"text": prompt_text}]
            }],
            inferenceConfig={
                "maxTokens": 10, 
                "temperature": 0
            }
        )
        
        ai_decision = response['output']['message']['content'][0]['text'].strip().upper()
        print(f"  🤖 Gemma Answer: {ai_decision}")
        
        return "SEND" in ai_decision

    except Exception as e:
        print(f"  ⚠️ Gemma Error: {e}")
        return True
        
def lambda_handler(event, context):
    print("--- STARTING SCRAPER RUN ---")
    
    req = urllib.request.Request(
        RSS_URL, 
        data=None, 
        headers={'User-Agent': 'Mozilla/5.0 (compatible; AptBot/AI-Edition)'}
    )
    
    try:
        with urllib.request.urlopen(req) as response:
            xml_data = response.read()
        print("RSS Feed fetched successfully.")
    except Exception as e:
        print(f"CRITICAL ERROR fetching RSS: {e}")
        return
        
    root = ET.fromstring(xml_data)
    
    entries = root.findall("atom:entry", NAMESPACES)
    print(f"RSS contains {len(entries)} total entries.")
    
    table = dynamodb.Table(TABLE_NAME)
    seen_ids = get_seen_posts(table)
    found_apartments = []
    
    today_str = datetime.now(timezone.utc).strftime('%Y-%m-%d')
    print(f"Filtering for date: {today_str}")
    
    skipped_db = 0
    skipped_date = 0
    
    for entry in entries:
        post_id = entry.find("atom:id", NAMESPACES).text
        title = entry.find("atom:title", NAMESPACES).text
        
        # --- 1. Check ID (Database) ---
        if post_id in seen_ids:
            skipped_db += 1
            continue
            
        print(f"\n🔎 Processing New Post: {title}")
        
        link = entry.find("atom:link", NAMESPACES).attrib['href']
        published_raw = entry.find("atom:updated", NAMESPACES).text
        published_date = published_raw.split('T')[0]
        content_html = entry.find("atom:content", NAMESPACES).text or ""
        
        # --- 2. Check Date ---
        if published_date != today_str:
            print(f"  ❌ Skipped (Old Date): Post is from {published_date}, we only want {today_str}")
            print(f"  🚫 Bedrock NOT invoked.")
            skipped_date += 1
            save_post_id(table, post_id, title)
            continue

        # --- 3. Check AI ---
        print(f"  🧠 Sending to Bedrock AI...")
        is_valid = ask_bedrock_analysis(title, content_html)
        
        if is_valid:
            print(f"  ✅ MATCH! Adding to email list.")
            found_apartments.append({'title': title, 'link': link})
        else:
            print(f"  🗑️ Rejected by AI criteria.")
            
        save_post_id(table, post_id, title)

    # --- SUMMARY LOGS ---
    print(f"\n--- REPORT ---")
    print(f"Total entries: {len(entries)}")
    print(f"Skipped (Already in DB): {skipped_db}")
    print(f"Skipped (Old Date): {skipped_date}")
    print(f"Sent to AI: {len(entries) - skipped_db - skipped_date}")
    
    if found_apartments:
        print(f"📧 Sending email with {len(found_apartments)} validated apartments.")
        
        message_lines = [f"✨ {len(found_apartments)} NEW APARTMENTS (AI VERIFIED)", "="*30, ""]
        
        for apt in found_apartments:
            message_lines.append(f"🏠 {apt['title']}")
            message_lines.append(f"🔗 {apt['link']}")
            message_lines.append("-" * 30)
            
        message_lines.append("\n\n\n") 
            
        email_body = "\n".join(message_lines)
        
        sns.publish(
            TopicArn=SNS_TOPIC_ARN,
            Message=email_body,
            Subject=f"✨ {len(found_apartments)} New Apartments (AI Filtered)"
        )
        return f"Sent {len(found_apartments)} alerts."
    else:
        print("🏁 Run complete. No new apartments matched criteria.")
        return "No email sent."