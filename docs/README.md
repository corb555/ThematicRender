
# LandWeaver

**LandWeaver** is a programmable spatial compositing and synthesis system for turning raw GIS data into refined cartographic output. It is designed for users who want more control than standard GIS styling typically provides, 
especially when working toward naturalistic, thematic, or presentation-quality maps. 

It combines GIS-aligned raster inputs, computed control masks, procedural texture, and an ordered compositing pipeline to create results that are smoother, richer, and more visually intentional than conventional raster workflows. 

## Why It’s Useful

For a **GIS analyst**, **LandWeaver** provides a structured way to combine multiple spatial layers, shape environmental signals, and build reproducible rendering logic using text-based configuration rather than one-off manual styling. It supports both continuous data such as elevation, precipitation, forest density, and lithology, and categorical rasters such as LANDFIRE. 

For an **illustrator or cartographer**, the main benefit is visual quality. **LandWeaver** helps reduce the blocky, stair-stepped appearance common in upscaled raster data and adds controlled texture, smoother transitions, and more natural surface variation. This makes it possible to produce maps that feel less mechanical and more polished. 

## Core Model

The system is built around five parts:

* **Sources** — GIS-aligned raster inputs
* **Factors** — computed masks and control signals
* **Surfaces** — color and material layers
* **Noise** — multi-scale procedural texture
* **Pipeline** — ordered compositing logic

This structure keeps projects modular, repeatable, and easier to tune. 

## Key Benefits

* **Better visual output** — smoother edges, fewer raster artifacts, and more natural transitions
* **More control** — shape how data is interpreted and blended instead of relying on fixed GIS symbology
* **Reproducible workflows** — define rendering logic in named, text-based configuration
* **Support for both analysis and presentation** — useful for analytical composites, thematic rendering, and high-end cartographic illustration
* **Scales to large datasets** — designed for both fast previews and heavy regional rendering jobs 

## In Short

**Name** helps bridge the gap between GIS analysis and finished visual design. It gives analysts a more programmable rendering workflow and gives illustrators better tools for turning raw spatial data into maps that look intentional, clear, and visually rich. 

---

## Key Features

### 1. Compositing Pipeline

The system is driven by a user configured compositing sequence:

* **Dynamic Data Sources:** Ingest any raster data (DEM, Precipitation, Forest Height, Lithology) and
  define custom transformation policies.
* **Logical Blending:** A library of atomic operations including `lerp_surfaces`, `alpha_over`, and
  `lerp_buffers` for sophisticated transitions.
* **Programmable Shading:** Multiplicative hillshade application with highlight/shadow protection logic to
  preserve color vibrancy and "clean" midtones in topographic relief.

### 2. Factor Library

Factors act as the control signals for the entire render, conditioned from physical sources into 0..1 masks:

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
* **Scientific Illustrators:** Visualizing complex environmental gradients as cohesive, naturalistic, and
  clearly defined biomes.

### Ideal for

* **Stylized relief / terrain texture generation** for print maps and high-end cartography
* **Biome and landcover rendering** from precipitation, canopy, lithology, and other sources
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

# Theme / Categorical Sources

A theme source is a spatial raster whose pixel values represent **discrete category IDs** rather than continuous
measurements. Example values might include `1 = Water`, `5 = Forest`, and `12 = Urban`. Because these values are
categorical labels, they cannot be meaningfully interpolated, averaged, or blended the way continuous rasters can.

A theme source is a `uint8` raster. Valid category IDs are `1-255`, and `0` is reserved
to mean **no category / background**.

Each theme source must have an associated QML file (QGIS layer style) that defines the renderer’s category registry.
The QML maps each class ID to:

- a text label
- an intended RGB color

If multiple theme sources are used in the same render configuration, their category labels must be globally unique so
that per-category configuration remains unambiguous.
Theme sources are defined in:

sources:
theme_composite:
label: "theme_composite"

surfaces:
theme_overlay:
source: theme
input_factor: null
required_factors: [theme_composite]
surface_builder: theme
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

## Render Settings 

| YAML Section          | Primary Connection          | Dependency Type  | Purpose                                           |
|:----------------------|:----------------------------|:-----------------|:--------------------------------------------------|
| **files**             | **source_specs**            | Physical Path    | Maps unique keys to static files (e.g., QML).     |
| **sources**           | **source_specs**            | Physical Path    | Maps keys to regional TIFFs (e.g., _DEM.tif).     |
| **source_specs**      | **factors**                 | Spatial Identity | Defines memory/dtype for math inputs.             |
| **logic**             | **factors**                 | Math Constants   | Stores `start/full` values and `noise_amp`.       |
| **factors**           | **surfaces**                | Functional Input | Transforms raw data into 0..1 alpha signals.      |
| **noise_profiles**    | **factors** & **modifiers** | Frequency Data   | Defines the organic "look" of biomes and grit.    |
| **theme_render**      | **factors**                 | Category Tuning  | Specifically drives the `theme_composite` factor. |
| **theme_smoothing**   | **theme_render**            | Geometry Fix     | Defines how blocky GIS pixels are rounded.        |
| **modifier_profiles** | **surfaces**                | Pixel Shift      | Defines RGB hue-shifting (mottling) profiles.     |
| **surfaces**          | **pipeline**                | RGB Source       | Combines Ramps + Mottling into image layers.      |
| **pipeline**          | **OUTPUT**                  | Composition      | The final list of steps to blend RGB with Alpha.  |

```mermaid
graph TD
    %% Physical Layer
    subgraph Physical_Layer [Resources]
        FILES[files] -->| | DS[source_specs]
        PREFIX[sources] -->| | DS
    end

    %% Logic Layer
    subgraph Logic_Layer [Calc]
        DS -->| | FACTORS[factors]
        NOISE[noise_profiles] -->| | FACTORS
        LOGIC[params] -->| | FACTORS
        NOISE -->| | MODS[modifier_profiles]
    end

    %% Theme Layer
    subgraph Theme_Layer [Themes]
        THEME[theme_render] -->| | FACTORS
        SMOOTH[theme_smoothing_specs] -->| | THEME
    end
    
    %% Material Layer
    subgraph Material_Layer [Surface]
        MODS -->| | SURFACES[surfaces]
        FACTORS -->| | SURFACES
    end

    %% Execution
    subgraph Execution_Layer [Comp Op]
        SURFACES -->|surface | PIPE[pipeline step]
        FACTORS -->|factor| PIPE
        BUFFER[Buffer]
    end
    
    %% Output
    subgraph Output [Raster]
        PIPE -->| | RASTER[Raster]
    end
    ```