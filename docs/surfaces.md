This guide explains how to configure the **Noise** and **Color Variation** systems to achieve naturalistic terrain textures.

---

# 1. NoiseProfile
The `NoiseProfile` defines the "geometry of randomness." It uses a Multi-Scale Gaussian approach, blending multiple layers of filtered noise to create organic patterns ranging from fine grit to massive patches.

### Attributes
| Attribute | Type | Range | Description |
| :--- | :--- | :--- | :--- |
| **`sigmas`** | `Tuple[float, ...]` | 0.5 — 50.0 | The "blur radius" of each noise layer. Small values create grit; large values create broad blobs. |
| **`weights`** | `Tuple[float, ...]` | 0.0 — 1.0 | The strength of each corresponding sigma layer. High weight on small sigmas = "toothy"; High weight on large = "cloudy." |
| **`stretch`** | `(V, H)` | 0.1 — 10.0 | Distorts noise. `(1.0, 1.0)` is circular. `(1.0, 5.0)` creates horizontal strata. |
| **`seed_offset`** | `int` | 0 — 9999 | Shakes the random seed. Use different offsets to prevent two profiles from looking identical. |

### Recommended Tuning
*   **For "Tooth" (0.5 - 1.5):** Use very small sigmas. This creates pixel-level variance that looks like sand or leaves.
*   **For "Organic Patches" (5.0 - 15.0):** Use medium sigmas. This creates "stands" of trees or geologic mineral patches.
*   **For "Regional Shifts" (20.0+):** Use large sigmas. This creates broad variety across the entire map.

---

# 2. HueVariationProfile
The `HueVariationProfile` defines how the noise affects the final color. It is used to add "mottle" or "staining" to otherwise flat color ramps.

### Attributes
| Attribute | Type | Range | Description |
| :--- | :--- | :--- | :--- |
| **`intensity`** | `float` | 0.0 — 50.0 | The "shove" in RGB space. 5-10 is subtle; 15-25 is heavy texturing. |
| **`shift_vector`** | `(R, G, B)` | -1.0 — 1.0 | The direction of the color shift. `1.0` adds color; `-1.0` removes it. |
| **`noise_id`** | `str` | N/A | Foreign key to the `NoiseProfile` you want to use. |

### Understanding the Shift Vector
*   **Additive (Positive):** Pushes the color toward a specific hue (e.g., `(1.0, 0.8, 0.0)` adds Yellow/Orange).
*   **Subtractive (Negative):** Removes color, creating "shadows" or "pockets" (e.g., `(-0.5, -0.5, -0.5)` creates neutral dark pits).
*   **Contrast:** Using a mix of positive and negative (e.g., `(-0.2, 1.0, -0.2)`) makes the primary color (Green) pop by darkening the other channels in the noise "valleys."

---

# 3. Sample Scenarios (Recipes)

### A. The "Toothy" Forest (Humid Vegetation)
High-contrast green with micro-detail to simulate tree crowns and shadows.
```python
NoiseProfile(id="forest_crunch", sigmas=(0.5, 1.5, 5.0), weights=(0.5, 0.3, 0.2))
HueVariationProfile(
    intensity=25.0, 
    shift_vector=(-0.4, 1.0, -0.2), # Bright Green peaks, Dark/Brown valleys
    noise_id="forest_crunch"
)
```

### B. Sandy Mottle (Arid Base)
Warm, broad organic staining to simulate mineral deposits in desert soil.
```python
NoiseProfile(id="biome", sigmas=(1.0, 4.0, 12.0), weights=(0.3, 0.4, 0.3))
HueVariationProfile(
    intensity=15.0, 
    shift_vector=(1.0, 0.7, 0.3), # Shifts toward Red/Yellow sandstone tones
    noise_id="biome"
)
```

### C. Geologic Strata (Rock/Lith)
Stretched noise to simulate sedimentary layering.
```python
NoiseProfile(id="strata", sigmas=(2.0, 10.0), weights=(0.5, 0.5), stretch=(1.0, 8.0))
HueVariationProfile(
    intensity=18.0, 
    shift_vector=(0.2, 0.2, 0.2), # Neutral grey/brown grit
    noise_id="strata"
)
```

### D. Fine Paper Tooth (Global Overlay)
A subtle, high-frequency grit to make the entire map feel like physical media.
```python
NoiseProfile(id="grit", sigmas=(0.5, 0.8), weights=(0.8, 0.2))
HueVariationProfile(
    intensity=6.0, 
    shift_vector=(-1.0, -1.0, -1.0), # Subtle dark "pits" in the paper
    noise_id="grit"
)
```

---

# 4. Troubleshooting
*   **"Digital" or "Grid" look:** Your sigmas are too small or your weights are too high on the small sigmas. Increase the medium sigma (3.0 - 8.0) weight to soften the pattern.
*   **Mottle is invisible:** High-luminance colors (like bright green or white snow) require higher `intensity` (20+) to overcome the base color brightness.
*   **Mottle looks like "Clouds":** You only have large sigmas. Add a sigma of `0.5` or `1.0` with a weight of `0.2` to add "grit" to the clouds.
*   **Identical Patterns:** If two layers (e.g. Forest and Soil) look like they share the same noise, check that they are using different `seed_offset` values or that the engine is correctly hashing their names.