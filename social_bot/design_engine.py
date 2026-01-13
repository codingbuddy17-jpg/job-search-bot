import os
import asyncio
from playwright.async_api import async_playwright
from jinja2 import Environment, FileSystemLoader

async def generate_image(data, output_filename="post.png"):
    """
    Renders the HTML template with data and saves it as an image.
    """
    # 1. Setup Jinja2 Environment
    template_dir = os.path.join(os.path.dirname(__file__), 'templates')
    env = Environment(loader=FileSystemLoader(template_dir))
    template = env.get_template('base.html')
    
    # 2. Render HTML
    html_content = template.render(**data)
    
    # Save formatted temp file inside the templates folder so relative paths (css/assets) work
    temp_file_path = os.path.join(template_dir, "temp.html")
    with open(temp_file_path, "w") as f:
        f.write(html_content)

    # 3. Use Playwright to take a screenshot
    async with async_playwright() as p:
        browser = await p.chromium.launch()
        page = await browser.new_page(viewport={'width': 1080, 'height': 1920})
        
        # Load the HTML
        file_path = f"file://{temp_file_path}"
        
        await page.goto(file_path)
        await page.wait_for_load_state('networkidle') # Wait for fonts/logo
        
        # Save Screenshot
        output_path = os.path.join("social_bot/output", output_filename)
        await page.screenshot(path=output_path)
        print(f"Generated: {output_path}")
        
        await browser.close()
    
    return output_path

if __name__ == "__main__":
    # Test Run
    test_data = {
        "subtitle": "CAREER TIPS",
        "title": "How to Ace Your Medical Coding Interview",
        "tip_1": "Brush up on ICD-10 guidelines and be ready to quote them.",
        "tip_2": "Bring your coding books (CPT, ICD) to demonstrate readiness.",
        "tip_3": "Practice explaining your logic for complex case studies."
    }
    asyncio.run(generate_image(test_data, output_filename="debug_icons.png"))
