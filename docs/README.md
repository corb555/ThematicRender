
# LandWeaver

**LandWeaver** is a programmable spatial compositing and synthesis system for turning raw GIS rasters into refined cartographic output.

It is designed for users who want more control than standard GIS styling typically provides, especially when working toward naturalistic, thematic, or presentation-quality maps. LandWeaver combines geospatial raster inputs, computed control masks, procedural texture, and an ordered compositing pipeline to produce results that are smoother, richer, and more visually intentional than conventional raster workflows.

![Sample LandWeaver render](https://github.com/corb555/LandWeaverServer/blob/6ace8f2d3b181e9f80a2c418ab9c9b67b1ee5af3/docs/images/grand%20canyon.png)

*Example output showing smooth biome transitions and naturalized categorical rendering from multiple GIS raster inputs.*

In this example, wetter North Rim conditions blend smoothly into more arid canyon terrain, while lithology-driven red rock areas remain visually coherent without blocky edges.

## Why LandWeaver

For a **GIS analyst**, LandWeaver provides a structured way to combine multiple spatial layers, derive environmental control signals, and build reproducible rendering logic using text-based configuration rather than one-off manual styling. It supports both continuous rasters such as elevation, precipitation, canopy, and lithology, and categorical sources such as themed landcover layers. 

For an **illustrator or cartographer**, the main benefit is visual quality. LandWeaver helps reduce the blocky, stair-stepped appearance common in upscaled raster data and adds controlled texture, smoother transitions, and more natural surface variation. The result is imagery that feels less mechanical and more crafted.

## High-Level Workflow

At a high level, a render works like this: 

1. **Sources**  
   Georeferenced raster inputs such as DEM, precipitation, canopy, lithology, slope, or categorical landcover.

2. **Factors**  
   Normalized control layers, usually in the range `0.0` to `1.0`, derived from source data. Factors determine where and how strongly an effect should be applied.

3. **Surfaces**  
   Render-ready visual layers such as terrain color, forest, snow, rock, or thematic overlays. Surfaces are usually created from raster sources using ramps, theme styling, and texture modifiers.

4. **Blend Operations**  
   Operations that combine factors and surfaces into visual results. For example, a canopy factor can blend meadow and forest surfaces based on vegetation density.

5. **Pipeline**  
   The ordered execution sequence that builds the final image step by step.

```mermaid
flowchart LR
    Sources[Sources]
    Functions[Functions]
    Factors[Factors]
    Surfaces[Surfaces]
    BlendOps[Blend Ops]
    Output[Output]

    Sources --> Functions
    Functions --> Factors

    Sources --> Surfaces

    Factors --> BlendOps
    Surfaces --> BlendOps

    BlendOps --> Output
````

## Key Benefits

* **Better visual output** — smoother edges, fewer raster artifacts, and more natural transitions
* **More control** — shape how data is interpreted and blended instead of relying on fixed GIS symbology
* **Reproducible workflows** — define rendering logic in named, text-based configuration
* **Useful for both analysis and presentation** — suitable for analytical composites, thematic rendering, and high-end cartographic illustration
* **Scales to large datasets** — designed for both fast previews and heavy regional rendering jobs 

## Core Capabilities

### Compositing Pipeline

LandWeaver is driven by a user-configured compositing sequence. 

* **Flexible raster inputs** — ingest geospatial rasters such as DEM, precipitation, canopy, or lithology
* **Logical blending** — combine layers using operations such as `lerp_surfaces`, `alpha_over`, and `lerp_buffers`
* **Programmable shading** — apply hillshade with highlight and shadow protection to preserve vibrancy and clean midtones

### Factor System

Factors are the control signals that drive the render. They transform physical source data into reusable masks and shaping layers. 

* **Apparent elevation logic** — displace or reshape environmental transitions to avoid rigid, artificial boundaries
* **Sensitivity-weighted shaping** — adjust scale, bias, contrast, and gamma-like sensitivity to control thresholds and softness
* **Reusable control layers** — derive once, then use throughout the pipeline for blending, masking, and material placement

### Surface Synthesis

Surfaces are the visual materials of the map, synthesized as RGB output layers. 

* **Ramp synthesis in physical units** — sample color ramps directly using physical values such as elevation in meters
* **Hue perturbation and texture** — add controlled variation, grit, and tactile surface character
* **Geometry cleanup** — transform blocky categorical or coarse raster inputs into smoother visual forms
* **Naturalization** — inject procedural variation to create patchiness, clumping, and more organic spatial texture

### Noise Library

LandWeaver includes a configurable multi-scale noise system for building organic texture. 

* **Weighted Gaussian scales** — combine fine and coarse structure in a controlled way
* **Anisotropic stretch** — create directional patterns for water, sediment, or wind-shaped surfaces
* **Iterative tuning** — calibrate texture response to match a desired geological or biological look

### Themed and Categorical Sources

LandWeaver supports both continuous rasters and discrete thematic rasters. For categorical sources, pixel values represent class IDs rather than continuous measurements, so they must be handled differently from elevation or precipitation. This is especially important when converting blocky classified rasters into smooth, visually coherent map surfaces. 

## Performance and Architecture

LandWeaver is built for both rapid iteration and large-scale production rendering. 

* **Multi-process rendering** — uses multiple processes to accelerate builds
* **Daemon-based architecture** — supports background execution and GIS-oriented caching
* **Windowed raster processing** — processes rasters a window at a time rather than reading entire datasets into memory
* **Fast preview workflow** — supports small previews for quick design iteration
* **Large-area builds** — scales to full regional renders with consistent output and reproducible pipelines

## Relationship to LandWeaverServer

LandWeaver is the user-facing front end for building and launching renders. The rendering engine runs in the background as **LandWeaverServer**, which handles the execution pipeline, raster processing, and output generation. This separation keeps the user workflow interactive while allowing the rendering system to scale independently.

## Audience

LandWeaver is intended for users who want more than standard GIS symbology and raster styling can provide. 

* **Digital cartographers** building high-end, illustrative, or artistic maps
* **GIS analysts and power users** seeking a programmable raster compositing workflow
* **Scientific illustrators** visualizing environmental gradients as coherent, naturalistic surfaces

### Ideal For

* **Stylized relief and terrain texture generation** for print maps and high-end cartography
* **Biome and landcover rendering** from precipitation, canopy, lithology, and related sources
* **Beautifying themed or classified rasters** without blocky edges
* **Tile-server textures and basemap layers** that need a richer, more handcrafted look
* **Large-area regional renders** where consistency, scale, and reproducibility matter
