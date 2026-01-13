# Base Image with Python
FROM python:3.9-slim

# Set working directory
WORKDIR /app

# Install system dependencies for Playwright & Chrome
# We use the official playwright install command which handles this best
RUN apt-get update && apt-get install -y \
    wget \
    gnupg \
    && rm -rf /var/lib/apt/lists/*

# Copy Requirements
COPY requirements.txt .

# Install Python Deps
RUN pip install --no-cache-dir -r requirements.txt

# Install Playwright Browsers (Chromium only to save space)
RUN playwright install chromium
RUN playwright install-deps chromium

# Copy Project Files
COPY . .

# Set Environment Variables (These will be protected in real deployment)
# You should ideally pass these via the Cloud Console, but for simplicity:
ENV PYTHONUNBUFFERED=1

# Command to run the bot
CMD ["python", "social_bot/telegram_bot.py"]
