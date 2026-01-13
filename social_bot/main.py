import os
import asyncio
import argparse
from content_generator import generate_social_content
from design_engine import generate_image

async def run_bot(topic):
    print(f"🤖 Bot Activated! Processing topic: '{topic}'")
    
    # 1. Generate Content
    print("✨ Generating expert content with AI...")
    content = generate_social_content(topic)
    
    if not content:
        print("❌ AI Generation failed.")
        return

    print("✅ Content Generated:")
    print(f"   Title: {content['title']}")
    print(f"   Subtitle: {content['subtitle']}")

    # 2. Generate Design
    print("🎨 Designing image...")
    # Use the content from AI to drive the design
    image_path = await generate_image(content, output_filename="final_post.png")
    
    print(f"\n🎉 DONE! Post created at: {image_path}")
    print("\n--- Caption ---")
    print(content['caption'])
    print("---------------")

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("topic", help="The topic to generate a post about")
    args = parser.parse_args()
    
    asyncio.run(run_bot(args.topic))
