from decor8ai.client import remove_objects_from_room, upscale_image, generate_designs
import base64, os
from PIL import Image
from io import BytesIO

print("Starting object removal and design generation...")

# Remove objects and upscale
empty_room_url = remove_objects_from_room(input_image_url='https://homestickystorage.blob.core.windows.net/website-static-media/IMG_0655.JPG')["info"]["image"]["url"]
# upscaled_data = base64.b64decode(upscale_image(empty_room_url, scale_factor=2)["info"]["upscaled_image"])

print(empty_room_url)

# Image.open(BytesIO(upscaled_data)).save(f"generated_designs_0.jpg")

# Generate and save designs
# os.makedirs("output-data", exist_ok=True)
# for i, img in enumerate(generate_designs(upscaled_data, room_type='familyroom', design_style='farmhouse', num_images=2)["info"]["images"]):
#     Image.open(BytesIO(base64.b64decode(img["data"]))).save(f"output-data/generated_designs_{i}.jpg")