from openai import OpenAI
import base64
import tiktoken
from PIL import Image
from pathlib import Path
import os
from dotenv import load_dotenv

# ----- CONFIG -----
INPUT_FOLDER = Path("C:/Users/myuan/Desktop/CHE/VP_text_image")
load_dotenv()
api_key = os.getenv("OPENAI_API_KEY")
client = OpenAI()# Prompt for GPT-4o to extract raw text
SYSTEM_PROMPT = (
    "You are a precise OCR assistant. Extract all readable text from the image as accurately as possible. "
    "Preserve line breaks and structure. Do not interpret or summarize. Do not omit anything. "
    "Just output the raw text exactly as it appears."
)

# Supported image extensions
IMAGE_EXTENSIONS = {".png", ".jpg", ".jpeg", ".webp", ".tiff", ".bmp"}

def encode_image_to_base64(image_path: Path) -> str:
    with open(image_path, "rb") as img_file:
        return base64.b64encode(img_file.read()).decode("utf-8")

def count_tokens_for_chat(messages, model="gpt-4o"):
    """Counts total tokens in a list of OpenAI chat messages."""
    encoding = tiktoken.encoding_for_model(model)
    num_tokens = 0
    for msg in messages:
        num_tokens += 4  # every message metadata structure
        for key, value in msg.items():
            num_tokens += len(encoding.encode(value))
    num_tokens += 2  # priming tokens
    return num_tokens

def estimate_image_tokens(image_path: Path) -> int:
    """Estimate token usage for an image input based on size (MP)."""
    with Image.open(image_path) as img:
        width, height = img.size
    megapixels = (width * height) / 1_000_000
    estimated_tokens = round(megapixels * 85)
    return estimated_tokens

def extract_text_from_image(client, image_path: Path) -> str:
    encoded_image = encode_image_to_base64(image_path)

    messages = [
        {"role": "system", "content": SYSTEM_PROMPT},
        {
            "role": "user",
            "content": [
                {"type": "text", "text": "Extract all visible text from this screenshot."},
                {"type": "image_url", "image_url": {
                    "url": f"data:image/png;base64,{encoded_image}"
                }}
            ]
        }
    ]
    # Count tokens before sending
    flat_messages = [
        {"role": "system", "content": SYSTEM_PROMPT},
        {"role": "user", "content": "Extract all visible text from this screenshot."}
    ]
    prompt_tokens = count_tokens_for_chat(flat_messages)

    response = client.chat.completions.create(
        model="gpt-4o",
        messages=messages,
        max_tokens=2048,
        temperature=0
    )

    usage = response.usage
    completion_tokens = usage.completion_tokens
    total_text_tokens = usage.total_tokens

    # Estimate image tokens
    image_tokens = estimate_image_tokens(image_path)
    total_estimated_tokens = total_text_tokens + image_tokens

    return (
        response.choices[0].message.content.strip(),
        total_estimated_tokens
    )

def process_folder(folder_path: Path):
    image_files = [f for f in folder_path.iterdir()
                   if f.suffix.lower() in IMAGE_EXTENSIONS and f.is_file()]
    if not image_files:
        print("No image files found in the folder.")
        return
    total_all = 0
    for image_path in image_files:
        print(f"Processing: {image_path.name}")
        try:
            text, total_tok = extract_text_from_image(client, image_path)
            output_path = image_path.with_suffix(".txt")
            with open(output_path, "w", encoding="utf-8") as f:
                f.write(text)
            print(f"Saved: {output_path.name}")
            print(f"Combined Total:       {total_tok}")
            total_all += total_tok
        except Exception as e:
            print(f"Failed to process {image_path.name}: {e}")
    print("\n=== Token Usage Summary ===")
    print(f"Estimated Grand Total:   {total_all}")
# ----- ENTRY POINT -----
if __name__ == "__main__":
    if not INPUT_FOLDER.is_dir():
        print(f"Invalid input folder path: {INPUT_FOLDER}")
    else:
        process_folder(INPUT_FOLDER)