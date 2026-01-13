import google.generativeai as genai
import json
import os

# Configure API Key (User needs to set this)
genai.configure(api_key=os.environ["GEMINI_API_KEY"])

import time

def generate_social_content(topic, complexity="Expert"):
    """
    Uses AI to generate a structured social media post from a topic.
    """
    model = genai.GenerativeModel('gemini-2.0-flash')
    
    prompt = f"""
    You are an expert Social Media Manager for 'CodingBuddy360', a premier Medical Coding training institute.
    
    Task: Create a value-packed social media post about: "{topic}"
    Target Audience: {complexity} level Medical Coders (Students to Professionals).
    Tone: Professional, Educational, Encouraging.
    
    Format your response STRICTLY as logical JSON with no markdown formatting.
    The JSON must have these exact keys:
    {{
        "subtitle": "Short Category Tag (e.g. EXAM TIPS, CAREER, GUIDELINES)",
        "title": "A Punchy, 5-7 word headline",
        "tip_1": "Tip 1 (Max 20 words)",
        "tip_2": "Tip 2 (Max 20 words)",
        "tip_3": "Tip 3 (Max 20 words)",
        "caption": "A detailed caption for the post including hashtags. (Max 100 words)"
    }}
    
    Ensure the tips are actionable and specific, not generic.
    """
    
    retries = 3
    for attempt in range(retries):
        try:
            print(f"🔄 Attempt {attempt+1}/{retries}: Requesting AI content...")
            response = model.generate_content(prompt)
            # Clean up json if model returns markdown ticks
            text = response.text.replace("```json", "").replace("```", "")
            return json.loads(text)
                
        except Exception as e:
            if "429" in str(e) or "Quota" in str(e):
                print(f"⚠️ Rate Limit Hit. Waiting 20 seconds before retry...")
                time.sleep(20)
            else:
                print(f"❌ Error generating content: {e}")
                return None
    
    print("⚠️ All retries failed. Falling back to MOCK DATA for demonstration.")
    return {
        "subtitle": "EXPERT INSIGHTS",
        "title": f"Mastering {topic} like a Pro",
        "tip_1": "Focus on the specific guidelines in Section I.C.",
        "tip_2": "Always cross-reference with the tabular list.",
        "tip_3": "Document your rationale for every complex code.",
        "caption": f"Deep dive into {topic}! 🚀 Medical coding is all about precision. Here are 3 expert tips to help you navigate this complex area. #MedicalCoding #CodingBuddy360 #AAPC"
    }

if __name__ == "__main__":
    # Test
    print(generate_social_content("Cardiology Coding"))
