import os
from PIL import Image
from concurrent.futures import ThreadPoolExecutor

# Global variables to track total sizes
total_before_size = 0
total_after_size = 0

# Function to compress a single image and print before and after file sizes
def compress_image(image_path, output_path=None, quality=70):
    global total_before_size, total_after_size
    
    try:
        # Get the file size before compression
        before_size = os.path.getsize(image_path)

        # Open the image
        img = Image.open(image_path)
        
        # If no output path provided, overwrite the original image
        if output_path is None:
            output_path = image_path
        
        # Compress and save the image
        img.save(output_path, 'JPEG', optimize=True, quality=quality)
        
        # Get the file size after compression
        after_size = os.path.getsize(output_path)

        # Update global totals
        total_before_size += before_size
        total_after_size += after_size

        # Convert sizes to KB for readability
        before_size_kb = before_size / 1024
        after_size_kb = after_size / 1024

        # Print the before and after file sizes for the image
        print(f"Compressed: {image_path} | Before: {before_size_kb:.2f} KB | After: {after_size_kb:.2f} KB | Reduction: {before_size_kb - after_size_kb:.2f} KB")

    except Exception as e:
        print(f"Error compressing {image_path}: {e}")

# Function to process images in batches
def process_images_in_batches(image_paths, batch_size=100, num_workers=4, quality=70):
    total_images = len(image_paths)
    
    # Loop through all images in batches
    for i in range(0, total_images, batch_size):
        batch = image_paths[i:i + batch_size]
        
        # Compress images in parallel within each batch
        with ThreadPoolExecutor(max_workers=num_workers) as executor:
            executor.map(lambda image: compress_image(image, quality=quality), batch)
        
        print(f"Batch {i // batch_size + 1} processed.")

# Function to get all images in the parent folder
def get_image_paths(parent_folder):
    image_paths = []
    for root, dirs, files in os.walk(parent_folder):
        for file in files:
            if file.lower().endswith(('.jpg', '.jpeg')):
                image_paths.append(os.path.join(root, file))
    return image_paths

# Main function
def compress_images_in_folder(parent_folder, batch_size=100, num_workers=4, quality=70):
    global total_before_size, total_after_size
    
    image_paths = get_image_paths(parent_folder)
    print(f"Found {len(image_paths)} images.")
    
    # Reset totals before processing
    total_before_size = 0
    total_after_size = 0
    
    # Process images in batches with parallel execution
    process_images_in_batches(image_paths, batch_size=batch_size, num_workers=num_workers, quality=quality)
    
    # Convert total sizes to MB for readability
    total_before_size_mb = total_before_size / (1024 * 1024)
    total_after_size_mb = total_after_size / (1024 * 1024)
    reduction_mb = total_before_size_mb - total_after_size_mb

    # Print total before and after file sizes
    print(f"\nTotal Size Before Compression: {total_before_size_mb:.2f} MB")
    print(f"Total Size After Compression: {total_after_size_mb:.2f} MB")
    print(f"Total Size Reduction: {reduction_mb:.2f} MB")

# Example usage
parent_folder = '/path/to/parent/folder'
batch_size = 100  # Process 100 images at a time
num_workers = 4   # Use 4 parallel threads per batch
quality = 70      # Set the quality level

# Compress images
compress_images_in_folder(parent_folder, batch_size, num_workers, quality)
