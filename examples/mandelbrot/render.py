#!/usr/bin/env python3
"""
Mandelbrot Set MQTT Renderer

Subscribes to mandelbrot/pixels/# on MQTT, accumulates pixel data in a numpy
array, and renders the result using matplotlib with the 'inferno' colormap.

The display updates every 10,000 pixels received and saves the final image
to mandelbrot.png when all 1,000,000 pixels (1000x1000) have arrived.

Usage:
    pip install -r requirements.txt
    python render.py [--host localhost] [--port 1883] [--size 1000]
"""

import argparse
import json
import signal
import sys

import matplotlib
matplotlib.use("Agg")  # Non-interactive backend for headless rendering
import matplotlib.pyplot as plt
import numpy as np
import paho.mqtt.client as mqtt


def main():
    parser = argparse.ArgumentParser(description="Mandelbrot MQTT renderer")
    parser.add_argument("--host", default="localhost", help="MQTT broker host")
    parser.add_argument("--port", type=int, default=1883, help="MQTT broker port")
    parser.add_argument("--size", type=int, default=1000, help="Image size (pixels)")
    parser.add_argument("--max-iter", type=int, default=256, help="Max iterations")
    parser.add_argument("--output", default="mandelbrot.png", help="Output file")
    args = parser.parse_args()

    size = args.size
    max_iter = args.max_iter
    total_pixels = size * size

    # Image buffer: iterations per pixel
    image = np.full((size, size), max_iter, dtype=np.int32)
    pixel_count = [0]  # Use list for mutability in closure

    def on_connect(client, userdata, flags, rc, properties=None):
        if rc == 0:
            print(f"Connected to MQTT broker at {args.host}:{args.port}")
            client.subscribe("mandelbrot/pixels/#")
        else:
            print(f"Connection failed with code {rc}")

    def on_message(client, userdata, msg):
        try:
            payload = json.loads(msg.payload.decode())
            x = int(payload.get("x", 0))
            y = int(payload.get("y", 0))
            iterations = int(payload.get("iterations", max_iter))

            if 0 <= x < size and 0 <= y < size:
                image[y, x] = iterations
                pixel_count[0] += 1

                # Progress update every 10,000 pixels
                if pixel_count[0] % 10_000 == 0:
                    pct = 100.0 * pixel_count[0] / total_pixels
                    print(f"Progress: {pixel_count[0]:,}/{total_pixels:,} pixels ({pct:.1f}%)")

                # Save when complete
                if pixel_count[0] >= total_pixels:
                    save_image(image, max_iter, args.output)
                    print(f"Image saved to {args.output}")
                    client.disconnect()

        except (json.JSONDecodeError, KeyError, ValueError) as e:
            pass  # Skip malformed messages

    def save_image(data, max_iter, filename):
        """Render the Mandelbrot set image with inferno colormap."""
        # Normalize: log scale for better contrast
        norm_data = np.where(
            data < max_iter,
            np.log1p(data.astype(np.float64)) / np.log1p(max_iter),
            0.0,  # Points in the set are black
        )

        fig, ax = plt.subplots(1, 1, figsize=(10, 10), dpi=100)
        ax.imshow(norm_data, cmap="inferno", origin="lower", extent=[-2, 1, -1.5, 1.5])
        ax.set_xlabel("Re(c)")
        ax.set_ylabel("Im(c)")
        ax.set_title(f"Mandelbrot Set ({max_iter} iterations)")
        plt.tight_layout()
        plt.savefig(filename, dpi=150, bbox_inches="tight")
        plt.close()
        print(f"Saved {filename}")

    # Set up MQTT client
    client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, client_id="mandelbrot-renderer")
    client.on_connect = on_connect
    client.on_message = on_message

    # Handle Ctrl+C gracefully - save partial image
    def signal_handler(sig, frame):
        print(f"\nInterrupted. Received {pixel_count[0]:,} pixels.")
        if pixel_count[0] > 0:
            save_image(image, max_iter, args.output)
        sys.exit(0)

    signal.signal(signal.SIGINT, signal_handler)

    print(f"Connecting to MQTT broker at {args.host}:{args.port}...")
    print(f"Expecting {total_pixels:,} pixels ({size}x{size})")
    client.connect(args.host, args.port, 60)
    client.loop_forever()


if __name__ == "__main__":
    main()
