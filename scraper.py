import urllib.request
import xml.etree.ElementTree as ET
import boto3
import os
import time
import json
from datetime import datetime, timezone

# --- CONFIGURATION ---
RSS_URL = "https://www.reddit.com/r/LeaseTakeoverNYC+NYCapartments/search.rss?q=%28%222BR%22+OR+%22Two+bed%22+OR+%22Two+bedrooms%22+OR+%222B%22+OR+%22Two+B%22%29&restrict_sr=1&sort=new"
BEDROCK_MODEL_ID = "google.gemma-3-12b-it"
NAMESPACES = {'atom': 'http://www.w3.org/2005/Atom'}

sns = boto3.client('sns')
dynamodb = boto3.resource('dynamodb')
bedrock = boto3.client('bedrock-runtime')

SNS_TOPIC_ARN = os.environ.get('SNS_TOPIC_ARN') 
TABLE_NAME = os.environ.get('TABLE_NAME') 

### Function Definition: ###
def get_seen_posts(table) -> set:
    response = table.scan()
    items = response.get('Items', [])
    return {item['post_id'] for item in items}

### Function Definition: ###
def save_post_result(table, post_id, title, status, reason) -> None:
    ttl = int(time.time()) + (14 * 24 * 60 * 60) 
    found_at = datetime.now(timezone.utc).isoformat()
    # We now save the status (SEND/SKIP) and the Reason
    table.put_item(Item={
        'post_id': post_id, 
        'title': title, 
        'found_at': found_at, 
        'status': status,
        'reason': reason,
        'ttl': ttl
    })

### Function Definition: ###
def ask_bedrock_analysis(title, body) -> tuple:
    # Revised prompt requesting JSON and clarifying the roommate rule
    prompt_text = f"""
    You are an AI Real Estate Agent filtering NYC apartments.
    
    TASK: Analyze this listing and provide a JSON response.
    
    CRITERIA FOR "SEND":
    1. 2-Bedroom unit (2BR, 2 Bed).
    2. ENTIRE UNIT ONLY. 
       - REJECT if user is offering a single room/subletting one room.
       - ACCEPT if current tenants (even if called "roommates") are moving out and the WHOLE unit is available.
    3. NOT asking for advice.
    4. Location is MANHATTAN only (Exclude Brooklyn, NJ, LIC, Roosevelt Island).
    
    LISTING:
    Title: {title}
    Body: {body}
    
    OUTPUT FORMAT:
    You must output strictly valid JSON with no markdown formatting:
    {{
        "decision": "SEND" or "SKIP",
        "reason": "Brief explanation of why"
    }}
    """

    try:
        response = bedrock.converse(
            modelId=BEDROCK_MODEL_ID,
            messages=[{"role": "user", "content": [{"text": prompt_text}]}],
            inferenceConfig={"maxTokens": 100, "temperature": 0}
        )
        
        raw_text = response['output']['message']['content'][0]['text'].strip()
        
        # Clean up code blocks if the model adds them
        if raw_text.startswith("```"):
            raw_text = raw_text.strip("`json").strip("`")

        result = json.loads(raw_text)
        decision = result.get("decision", "SKIP").upper()
        reason = result.get("reason", "No reason provided")
        
        print(f"  🤖 Gemma: {decision} | Reason: {reason}")
        return decision, reason

    except Exception as e:
        print(f"  ⚠️ Gemma Error: {e}")
        # Default to sending if error, but note the error
        return "SEND", f"Error: {str(e)}"

### Function Definition: ###
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
    except Exception as e:
        print(f"CRITICAL ERROR fetching RSS: {e}")
        return
        
    root = ET.fromstring(xml_data)
    entries = root.findall("atom:entry", NAMESPACES)
    
    table = dynamodb.Table(TABLE_NAME)
    seen_ids = get_seen_posts(table)
    found_apartments = []
    
    today_str = datetime.now(timezone.utc).strftime('%Y-%m-%d')
    
    for entry in entries:
        post_id = entry.find("atom:id", NAMESPACES).text
        title = entry.find("atom:title", NAMESPACES).text
        
        if post_id in seen_ids:
            continue
            
        print(f"\n🔎 New Post: {title}")
        
        link = entry.find("atom:link", NAMESPACES).attrib['href']
        published_date = entry.find("atom:updated", NAMESPACES).text.split('T')[0]
        content_html = entry.find("atom:content", NAMESPACES).text or ""
        
        if published_date != today_str:
            print(f"  ❌ Skipped (Old Date)")
            save_post_result(table, post_id, title, "SKIP", "Old Date")
            continue

        print(f"  🧠 Sending to Bedrock AI...")
        decision, reason = ask_bedrock_analysis(title, content_html)
        
        save_post_result(table, post_id, title, decision, reason)
        
        if decision == "SEND":
            print(f"  ✅ MATCH!")
            found_apartments.append({'title': title, 'link': link, 'reason': reason})
        else:
            print(f"  🗑️ Rejected.")

    if found_apartments:
        print(f"📧 Sending email with {len(found_apartments)} validated apartments.")
        message_lines = [f"✨ {len(found_apartments)} NEW APARTMENTS", "="*30, ""]
        
        for apt in found_apartments:
            message_lines.append(f"🏠 {apt['title']}")
            message_lines.append(f"💡 AI Note: {apt['reason']}")
            message_lines.append(f"🔗 {apt['link']}")
            message_lines.append("-" * 30)
            
        sns.publish(
            TopicArn=SNS_TOPIC_ARN,
            Message="\n".join(message_lines),
            Subject=f"✨ {len(found_apartments)} New Apartments Found"
        )
        return f"Sent {len(found_apartments)} alerts."
    
    return "No new matches."