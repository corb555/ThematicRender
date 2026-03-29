# Thematic Render

**Thematic Render** is a fully programmable spatial compositing and synthesis engine designed to transform raw
geographic data into high-end cartographic art. It provides a versatile framework where users define
data drivers, mathematical logic, and execution pipelines to achieve specific aesthetics—ranging from soft,
organic naturalism to crisp, anti-aliased thematic layouts.

Unlike standard GIS tools that often produce clinical or "blocky" results, Thematic Render focuses on the
**physics of aesthetics**. It utilizes a sophisticated toolkit of procedural noise, multi-scale Gaussian
filters, and non-linear signal shaping to simulate natural transitions, geological grit, and biological density.

## The Organic Transition

Thematic Render solves the "Low-Res Data Problem" common in spatial visualization. Through a two-phase processing
model, the engine eliminates the "stair-step" artifacts of upscaled data:

## Key Features

### 1. Compositing Pipeline

The system is driven by a user configured compositing sequence:

* **Dynamic Data Drivers:** Ingest any raster data (DEM, Precipitation, Forest Height, Lithology) and
  define custom transformation policies.
* **Logical Blending:** A library of atomic operations including `lerp_surfaces`, `alpha_over`, and
  `lerp_buffers` for sophisticated transitions.
* **Programmable Shading:** Multiplicative hillshade application with highlight/shadow protection logic to
  preserve color vibrancy and "clean" midtones in topographic relief.

### 2. Factor Library

Factors act as the control signals for the entire render, conditioned from physical drivers into 0..1 masks:

* **Apparent Elevation Logic:** Procedural displacement of elevation and moisture inputs to create natural,
  wandering biome contacts and eliminate artificial "bathtub rings."
* **Sensitivity-Weighted Shaping:** Per-factor Scale, Bias, Contrast, and Sensitivity (Gamma) controls to
  tune the "threshold" and "lushness" of environmental transitions.
* **Topological Precedence:** A priority-based "Melt and Claim" system that ensures high-precedence
  features (like Water or Rock) correctly carve through lower-precedence materials (like Forest).

### 3. Surface Library

Surfaces are the "Materials" of the map, synthesized as RGB data through various providers:

* **Ramp Synthesis:** Samples 1D color ramps using physical meters (`elev_m`) for sub-pixel precision,
  eliminating the banding and data loss of traditional 8-bit normalization.
* **Hue Perturbation (Mottle):** Noise-driven RGB hue shifting that adds tactile "surface tooth" and
  geological grit to otherwise flat digital gradients.
* **Geometry Cleanup:** Discrete categorical and continuous data are **materialized** into smooth probability fields,
  turning blocky 30m pixels into elegant, rounded curves.
* **Synthesis & Naturalization:** High-frequency procedural noise is injected to create "clumpy" vegetation, "patchy"
  mineral deposits, and "rippling" water surfaces, simulating the non-uniformity found in nature.

### 4. Noise Library

The engine features a highly configurable noise library for creating unique organic textures:

* **Weighted Gaussian Sigmas:** Define noise profiles using multiple scales of Gaussian-filtered white noise.
  This allows for the blending of high-frequency "grit" and "fuzz" with large, sweeping organic masses.
* **Anisotropic Stretch:** Apply directional stretch parameters to simulate flowing water, sedimentary rock
  layers, or wind-swept vegetation patterns.
* **Real-time Calibration:** Designed for iterative design, allowing users to tune frequency response and
  weights to match specific geological or biological characteristics.

### 5. High-Performance Architecture

To ensure absolute stability when processing massive (50GB+) regional datasets:

* **Dimensional Firewall:** A strict structural contract that isolates 3D storage from 2D compute,
  mathematically preventing the broadcasting bugs common in square-tile raster processing.
* **Materialization Firewall:** Data is unpacked into a 2D compute-safe zone immediately upon entry,
  stripping
  storage-related artifacts before the math begins.
* **IPC-Ready Buffer Pools:** A "Shared Memory" architecture that decouples Reader, Worker, and Writer tasks
  for high-performance, multi-core processing.

## Audience

* **Digital Cartographers:** Moving beyond standard GIS styling into high-end, illustrative, and artistic
  map design.
* **GIS Analysts & Power Users:** Seeking a high-performance, programmable alternative to traditional raster
  calculators for complex multi-layer compositing.
* **World Builders & Game Designers:** Requiring realistic, data-driven terrain textures or clean, organic
  splat-maps derived from physical or procedural heightmaps.
* **Scientific Illustrators:** Visualizing complex environmental gradients as cohesive, naturalistic, and
  clearly defined biomes.

### Ideal for

* **Stylized relief / terrain texture generation** for print maps and high-end cartography
* **Biome and landcover rendering** from precipitation, canopy, lithology, and other drivers
* **Themed/classified raster beautification** (e.g., LandFire/EVT-style palettes) without blocky edges
* **Tile-server textures and basemap layers** where you want “handcrafted” richness at scale
* **Large-area regional renders** where you need consistent look + reproducible pipelines across huge datasets

---

## Components

### Procedural surface synthesis

* **Factor displacement** using procedural noise to create natural, wandering biome boundaries.
* **Hue perturbation (“mottle”)** to add controlled surface variation even on flat terrain.
* **Single-pass compositing** of multiple material layers into a unified RGBA output.

### Thematic refinement and generalization

* **Morphological “melt & claim”** smoothing to turn blocky thematic rasters into flowing shapes.
* **Hole healing** to repair speckle noise and “bullet holes” in upscaled inputs.
* **QML palette ingestion** for themed/classified rasters.

### Performance and reliability

* A strict compute contract (“dimensional firewall”) that prevents common tile/broadcasting bugs.
* IPC-ready buffer pools for a Reader/Worker/Writer architecture with zero-copy views.
* Dual-mode execution: deterministic sequential runs for debugging, parallel runs for throughput.

## Game Developers

For a game developer, the output of **Thematic Render** is essentially a **"Procedural Master
Texture"** that solves several major headaches in modern world-building.

While standard GIS software produces maps for analysis, this engine produces **Assets**. Here is how a game
developer can use the GeoTIFF output:

### 1. "Clean" Albedo Maps (Base Color)

Most satellite imagery (like Sentinel or Landsat) has baked-in shadows, atmospheric haze, and seasonal "noise."
Game engines want **Albedo**—pure color without lighting information.

* **The Benefit:** Because Thematic Render builds the color from the ground up using ramps and factors, the
  output is a perfectly clean "Diffuse" map.
* **The Result:** The developer can apply their own dynamic lighting and day/night cycles in-engine without
  the map looking "dirty" or having pre-existing shadows that conflict with the game's sun.

### 2. High-Fidelity Splat Maps (Weight Maps)

Game engines use "Splat Maps" to tell the terrain shader where to paint grass, rock, or sand.

* **The Usage:** Instead of outputting the final RGB image, a developer can use the **Factor Engine** to export
  the individual masks (like `lith`, `canopy`, and `moisture`).
* **The Benefit:** These aren't just noisy masks; they are **geometrically cleaned** (no stair-steps) and
  **organically jittered**.
* **Asset Placement:** The developer can feed the `canopy` factor into a "Procedural Foliage Volume" in Unreal
  Engine. This ensures that 3D trees are only spawned exactly where the high-res canopy data says they should be.

### 3. Integrated Topographic Detail

The engine’s ability to "Melt and Claim" categorical data (Theme Smoothing) is vital for games.

* **The Problem:** In many games, the transition from a forest to a lake is a hard, pixelated line.
* **The Solution:** Thematic Render produces the "Transition Zone." The output TIFF contains soft, organic
  "Shorelines" and "Ecotones."
* **The Result:** A developer can import the GeoTIFF, and the "Shoreline Fade" we built for the water would
  automatically look like wet, receding sand in the game engine's shader.

### 4. World Partitioning and Scale

Modern engines (Unreal Engine 5’s World Partition) work by breaking worlds into a grid of tiles.

* **The Usage:** Because Thematic Render uses a **Tile-Based Engine Contract**, it can output a 50km x 50km
  world in chunks that match the game engine's tile size perfectly.
* **Accuracy:** The use of **Physical Meters (`elev_m`)** ensures that the texture perfectly aligns with the
  Heightmap (DEM) being used to deform the terrain mesh. 1 pixel in the texture corresponds exactly to 1 meter in the
  game world.

### 5. The "Technical Bridge"

1. **Render** the project using Thematic Render.
2. **Export** as a standard 16-bit TIFF or PNG (using a simple `gdal_translate` or by adding a `.png` encoder
   to our `write_task`).
3. **Import** into a terrain tool like **World Machine**, **Gaea**, or directly into **Unreal Engine's Landscape
   Mode**.

### Summary

Thematic Render allows a game developer to skip the "Manual Painting" phase of world-building. Instead of
hand-painting where the red rocks are in Sedona, they feed in the real-world Lithology data, and the engine
produces a **production-ready terrain texture** that looks like it was painted by a concept artist.

## Design

### Engine Contract (Firewall Architecture):

Storage (3D): Rasters are stored in Shared Memory as (H, W, Bands) or (B, H, W).
Compute (2D): render_task act as a firewall. It rehydrates data from SHM and strictly squeezes all single-band inputs
and validity masks to 2D (H, W) before calling engines.
Safety: This prevents the 384 3 384 3 broadcasting bug common in square-tile NumPy processing.
Output: blend_window transposes the final (H, W, 3) buffer back to (3, H, W) uint8 for the writer.

### Engine States:

Factor Engine: Demand-driven (derives requirements from pipeline). Uses FactorLibrary with a @spatial_factor decorator
that manages execution timers and restores 3D shapes.
Surface Engine: Manages a "Modifier Chain" (e.g., Mottle) and Samples 1D color ramps using physical meters (elev_m) to
preserve precision and negative values.
ConfigMgr: A "Fused Truth Store" built at startup. Merges settings.py (logic/specs) with YAML (paths/overrides). Uses
get_logic(key), get_spec(key), and path(key) accessors.

# Theme / Categorical Drivers

A theme driver is a spatial raster whose pixel values represent **discrete category IDs** rather than continuous
measurements. Example values might include `1 = Water`, `5 = Forest`, and `12 = Urban`. Because these values are
categorical labels, they cannot be meaningfully interpolated, averaged, or blended the way continuous rasters can.

A theme driver is a `uint8` raster. Valid category IDs are `1-255`, and `0` is reserved
to mean **no category / background**.

Each theme driver must have an associated QML file (QGIS layer style) that defines the renderer’s category registry.
The QML maps each class ID to:

- a text label
- an intended RGB color

At load time, the registry parses the QML and builds lookup tables (LUTs), allowing the renderer to efficiently convert
theme IDs into RGB thematic output.

If multiple theme drivers are used in the same render configuration, their category labels must be globally unique so
that per-category configuration remains unambiguous.
Theme drivers are defined in:

drivers:
theme_composite:
label: "theme_composite"

surfaces:
theme_overlay:
driver: theme
coord_factor: null
required_factors: [theme_composite]
provider_id: theme
files: [theme_qml]
config: "EVT_theme.qml"
desc: >
Categorical colors for specific features (water, rock, glacier)
defined in QML.

Each category can also receive its own cleanup and rendering settings:

theme_smoothing_specs:
theme_smoothing:
water: { smoothing_radius: 3.0 }
rock: { smoothing_radius: 6.0 }
volcanic: { smoothing_radius: 6.0 }
glacier: { smoothing_radius: 6.0 }
playa: { smoothing_radius: 6.0 }
outwash: { smoothing_radius: 6.0 }
_default_: { smoothing_radius: 3.0 }

theme_render:
categories:
water:
enabled: true
blur_px: 3.0
noise_amp: 0.0
contrast: 5.0
max_opacity: 0.8

    rock:
      enabled: true
      blur_px: 5.0
      noise_amp: 0.9
      contrast: 0.8
      max_opacity: 0.6
      noise_id: geology

    volcanic:
      enabled: true
      noise_amp: 0.5
      contrast: 2.0
      max_opacity: 0.8

    glacier:
      enabled: true
      noise_amp: 0.6
      contrast: 1.0
      max_opacity: 0.9

    playa:
      enabled: true
      blur_px: 8.0
      noise_amp: 0.95
      contrast: 0.5
      max_opacity: 0.4

    outwash:
      enabled: false