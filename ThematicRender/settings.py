from __future__ import annotations

from dataclasses import field
from typing import FrozenSet, Dict, Final, Any

import numpy as np

from ThematicRender.keys import (SurfaceKey, DriverKey, FactorKey, FileKey, DriverSpec, SurfaceSpec, \
                                 _BlendSpec, FactorSpec, _zeros, _ones, NoiseProfile,
                                 SurfaceModifierProfile)
#setttings.py
BLEND_PIPELINE = [
    _BlendSpec(
    # ARID LAYER
    desc="Mix ARID_BASE and ARID_RED_BASE using lithology data to create "
         "the ARID_COMPOSITE surface", comp_op="lerp_surfaces", factor_nm="lith",scale=1.8,
    input_surfaces=[SurfaceKey.ARID_BASE, SurfaceKey.ARID_RED_BASE],
    output_surface=SurfaceKey.ARID_COMPOSITE, enabled=True
), _BlendSpec(
    desc="Create the Canvas buffer with the ARID_COMPOSITE surface",
         enabled=True, comp_op="create_buffer", factor_nm=None,
    input_surfaces=[SurfaceKey.ARID_COMPOSITE]
), _BlendSpec(
    desc="Add ARID_VEGETATION to the arid region using the canopy "
         " mask.",
    enabled=True, comp_op="lerp", factor_nm="canopy", input_surfaces=[SurfaceKey.ARID_VEGETATION],
scale=2.3, contrast=0.7),
    # HUMID LAYER
    _BlendSpec(
        desc="Mix HUMID_BASE and ARID_RED_BASE using lithology data to create "
             "the HUMID_COMPOSITE surface", comp_op="lerp_surfaces", factor_nm="lith",scale=1.8,
        input_surfaces=[SurfaceKey.HUMID_BASE, SurfaceKey.ARID_RED_BASE],
        output_surface=SurfaceKey.HUMID_COMPOSITE, enabled=True
    ),
    _BlendSpec(
        desc="Create the humid buffer with HUMID_COMPOSITE.",
        comp_op="create_buffer", factor_nm=None, input_surfaces=[SurfaceKey.HUMID_COMPOSITE],
        buffer="humid", enabled=True
    ), _BlendSpec(
        desc="Add humid vegetation to the humid buffer "
             "using "
             "the canopy mask.", comp_op="lerp", enabled=True, factor_nm="canopy",
        input_surfaces=[SurfaceKey.HUMID_VEGETATION], buffer="humid", ),
    # MERGE HUMID AND ARID
    _BlendSpec(
        desc="Merge the Canvas (Arid) buffer and Humid buffers using the "
             "moisture gradient.", enabled=True, comp_op="lerp_buffers",
        factor_nm="moisture", merge_buffer="humid", bias=-0.30
    ), _BlendSpec(
        desc="Add thematic classes (water, rock, ice) onto the terrain using "
             "smoothed masks.", enabled=True, comp_op="alpha_over", factor_nm="theme",
        input_surfaces=[SurfaceKey.THEME_OVERLAY]
    ), _BlendSpec(
        desc="Mask in high-altitude snow and ice color ramps based on the jittered elevation "
             "snowline.",
        enabled=False, comp_op="lerp", factor_nm="snow", input_surfaces=[SurfaceKey.SNOW]
    ), _BlendSpec(
        desc="Simulate deep water by applying a darkening gradient to lake and river interiors "
             "based "
             "on proximity to shore.", comp_op="apply_zonal_gradient", enabled=False,
        factor_nm="water_depth", mask_nm="water", params={
            "color_0": [82, 90, 105],  # Shallow color
            "color_1": [58, 64, 74]  # Deep color
        }
    ),_BlendSpec(
        desc="Composite thematic water using the Shoreline Fade.",
        enabled=True,
        comp_op="alpha_over",
        factor_nm="water_alpha", # Use the fade instead of the solid mask
        input_surfaces=[SurfaceKey.THEME_OVERLAY]
    ),
    _BlendSpec(
        desc="Apply wave structure (shadows) to the water surface.",
        comp_op="multiply",
        factor_nm="water_ripples",
        buffer="canvas",
        enabled=True
    ),
    _BlendSpec(
        desc="Inject sun glints (highlights) onto wave crests.",
        comp_op="add_specular_highlights",
        factor_nm="water_glint",
        buffer="canvas",
        params={
            "color": [255, 255, 255],
            "intensity": 1.2    # Overdrive the brightness
        },
        enabled=True
    ),
    _BlendSpec(
        desc="Add hillshades", enabled=True, comp_op="multiply",
        factor_nm="hillshade", buffer="canvas"
    ), _BlendSpec(
        desc="Output the canvas buffer",
        enabled=True, comp_op="write_output", buffer="canvas"
    ), ]

_SURFACE_TO_FILE_KEYS: Dict["SurfaceKey", FrozenSet[str]] = field(
    default_factory=lambda: {
        SurfaceKey.ARID_BASE: frozenset({"arid_base"}),
        SurfaceKey.ARID_VEGETATION: frozenset({"arid_vegetation"}),
        SurfaceKey.ARID_COMPOSITE: frozenset({"arid_composite"}),
        SurfaceKey.ARID_RED_BASE: frozenset({"arid_red_base"}),

        SurfaceKey.HUMID_BASE: frozenset({"humid_base"}),
        SurfaceKey.HUMID_VEGETATION: frozenset({"humid_vegetation"}),
        SurfaceKey.THEME_OVERLAY: frozenset({"theme_qml"}),
    }, init=False, repr=False, )

FACTOR_SPECS: list[FactorSpec] = [
    FactorSpec(
    name="elev",  function_id="elevation_norm", default_factory=_zeros,
    drivers=frozenset({DriverKey.DEM}), desc="Normalized 0..1 elevation", required_noise="biome"
),
    FactorSpec(
    name="elev_m",
    default_factory=_zeros,
    function_id="elevation_raw",
    drivers=frozenset({DriverKey.DEM}),
    desc="Raw physical elevation in meters for ramp sampling."
),
    FactorSpec(
        name="water_alpha",
        function_id="water_alpha",
        default_factory=_ones,
        drivers=frozenset({DriverKey.WATER_PROXIMITY, DriverKey.THEME}),
        desc="Fades water opacity at the shoreline to reveal the bottom."
    ),
    FactorSpec(
    name="moisture",  function_id="moisture",
    default_factory=_ones, drivers=frozenset({DriverKey.PRECIP}), required_noise="biome",
    desc="Environmental gradient mask (Arid vs Humid) derived from precipitation data."
), FactorSpec(
    name="canopy", function_id="canopy",
    default_factory=_ones, drivers=frozenset({DriverKey.FOREST}), required_noise="forest",
    desc="Biological mask defining vegetation density (Forest vs Meadow)."
), FactorSpec(
    name="lith",  function_id="lith", required_noise="geology",
    default_factory=_zeros, drivers=frozenset({DriverKey.LITH}),
    desc="Organic transition mask for red-rock lithology regions."
), FactorSpec(
    name="snow",  function_id="snow",
    default_factory=_zeros, drivers=frozenset({DriverKey.DEM}),
    desc="High-contrast mask for permanent snow and ice based on elevation jitter."
), FactorSpec(
    name="hillshade",
    function_id="hillshade", default_factory=_ones, drivers=frozenset({DriverKey.HILLSHADE}),
    desc="A raster representing modeled topographic shading."
), FactorSpec(
    name="theme",  function_id="theme",
    default_factory=_zeros, drivers=frozenset({DriverKey.THEME}),
    files=frozenset({FileKey.THEME_QML}),
    desc="Smoothed opacity mask for thematic LandFire categories (glaciers, water, etc.)."
), FactorSpec(
    name="water", function_id="water_mask",
    default_factory=_zeros, drivers=frozenset({DriverKey.THEME}),
    desc="Binary mask for water bodies used for specialized water effects."
), FactorSpec(
    name="water_depth",
    function_id="water_depth", default_factory=_zeros,
    drivers=frozenset({DriverKey.WATER_PROXIMITY}),
    desc="Distance-based gradient inside water bodies to simulate bathymetric darkening."
), FactorSpec(
    name="water_glint",
    function_id="water_glint", default_factory=_zeros, drivers=frozenset({DriverKey.THEME}),
    required_noise="water", desc="High-frequency specular highlights for water surfaces.",
    required_factors=("water",),
    ),
    FactorSpec(
        name="water_ripples",
        function_id="water_ripples",
        default_factory=_ones,
        drivers=frozenset({DriverKey.THEME}),
        required_noise="water",
        required_factors=("water",),
        desc="Base wave structure for water shading."
    ),
]

DRIVER_SPECS: Final[dict["DriverKey", DriverSpec]] = {
    DriverKey.DEM: DriverSpec(dtype=np.float32, halo_px=64),
    DriverKey.PRECIP: DriverSpec(dtype=np.float32, halo_px=64),
    DriverKey.LITH: DriverSpec(dtype=np.float32, halo_px=64, cleanup_type = "continuous", smoothing_radius= 8.0),
    DriverKey.HILLSHADE: DriverSpec(dtype=np.float32, halo_px=64),
    DriverKey.FOREST: DriverSpec(dtype=np.float32, halo_px=64, cleanup_type = "continuous",smoothing_radius= 8.0),
    DriverKey.WATER_PROXIMITY: DriverSpec(dtype=np.float32, halo_px=64,  cleanup_type = "continuous",smoothing_radius= 15.0),
    DriverKey.THEME: DriverSpec(dtype=np.uint8, halo_px=64, cleanup_type = "categorical"),
}

DRIVER_LOGIC_PARAMS: Final[dict[str, dict[str, Any]]] = {
    "dem": {
        "start": 0, "full": 4000, "noise_amp": 0.1, "contrast": 1.0, "max_opacity": 1.0
    },
    "water": {
        "glint_scale": 6.0,
        "glint_floor": 0.5,        # Higher floor = sparser, sharper sparkles
        "glint_sensitivity": 3.0,
        "ripple_scale": 3.0,      # Larger scale for the general wave shapes
        "ripple_intensity": 0.2,   # How much the waves darken the water
        "max_depth_px": 300.0,
        "depth_sensitivity": 2.5
    },
    "lith": {
        "band": 1, "start": 0, "full": 100, "blur_px": 12.0,"max_opacity": 1.0,
       # "noise_amp": 0.5, "contrast": 2.2, "max_opacity": 0.8
        #"noise_amp": 0.7, "contrast": 1.5,"noise_atten_power": 0.5,
        "noise_amp": 0.55,  "contrast": 1.2,  "sensitivity": 0.8,
    },
    "playa": {
        "blur_px": 4.0, "noise_amp": 0.6, "contrast": 1.5, "max_opacity": 0.8
    },
    "rock": {
         "noise_amp": 0.25, "noise_atten_power": 0.5,
    },
    "volcanic": {
        "noise_amp": 0.2, "noise_atten_power":1.0,"max_opacity": 0.8
    },

    "precip": {
        "start": 200, "full": 500, "noise_amp": 0.15, "noise_atten_power": 1.2, "contrast": 2.5
    },
    "forest": {
        # good "start": 0, "full": 40, "noise_amp": 0.2, "noise_atten_power": 0.7, "contrast": 1.0,  "sensitivity": 0.7,
        "start": 0, "full": 40, "noise_amp": 0.45, "noise_atten_power": 0.5, "contrast": 1.2,  "sensitivity": 1.5, "max_opacity": 0.9
    },
    "hillshade": {
        "strength": 0.8, "shadow_start": 0, "shadow_end": 0.235, "protect_shadows": 0.2,"protect_highlights": 0.1,
        "highlight_start": 0.86, "highlight_end": 1.0,
    }
}

NOISE_PROFILES: Final[Dict[str, NoiseProfile]] = {
    "biome": NoiseProfile(
        id="biome", sigmas=(1.0, 3.0, 8.0), weights=(0.7, 0.2, 0.1),
        desc="Organic noise for biome transitions and broad land-cover variety."
    ),"geology": NoiseProfile(
        id="geology", sigmas=(2.0, 40.0, 80.0),
        # 2.0 (Grit), 10.0 (Clumps), 40.0 (Macro geological units)
        weights=(0.1, 0.5, 0.4),  # Favor the larger sigmas for broad transitions
        desc="Macro-scale organic noise for geological and lithological boundaries."
    ),
    "water": NoiseProfile(
        id="water", sigmas=(0.8, 1.5, 3.0), weights=(0.6, 0.3, 0.1), stretch=(1.0, 4.0),
        seed_offset=1,
        desc="Horizontally stretched noise to simulate water surface patterns and liquid flow."
    ), "fine_mottle": NoiseProfile(
        id="fine_mottle", sigmas=(1.0, 2.0), weights=(0.8, 0.2),
        desc="High-frequency granular noise for simulating surface grit and fine soil texture."
    ), "forest": NoiseProfile(
        id="forest", sigmas=(0.8, 3.5, 12.0), weights=(0.3, 0.5, 0.2), stretch=(1.0, 1.0),
        desc="Multi-scale noise blending fine tooth with medium clumps to simulate organic forest "
             "canopy."
    ),
}

# A catalog of different "looks" for surfaces
SURFACE_MODIFIER_PROFILES: Final[Dict[str, SurfaceModifierProfile]] = {
    "water": SurfaceModifierProfile(
        intensity=10.0, shift_vector=(-0.2, 0.2, 1.0), noise_id="biome",
        desc="Cooling hue shifts to provide teal and deep blue variety to water bodies."
    ),
    "arid_mottle": SurfaceModifierProfile(
        intensity=15.0, shift_vector=(1.0, 0.8, 0.5), noise_id="biome",
        desc="Warm sandstone and tan staining for arid soil and desert regions."
    ), "forest_mottle": SurfaceModifierProfile(
        intensity=35.0, shift_vector=(0.1, 1.0, 0.2), noise_id="forest",
        desc="High-contrast canopy variation with vibrant green peaks and deep neutral shadows."
    ), "humid_vegetation": SurfaceModifierProfile(
        intensity=12.0, shift_vector=(0.8, 1.0, -0.5), noise_id="biome",
        desc="Chlorophyll-focused variation for lush, moisture-rich vegetation layers."
    ), "arid_vegetation": SurfaceModifierProfile(
        intensity=15.0, shift_vector=(1.0, 0.8, 0.2), noise_id="biome",
        desc="Desaturated, earthy color shifts for dry-climate scrub, sagebrush, and dormant grass."
    ), "arid_base_mod": SurfaceModifierProfile(
        intensity=8.0, shift_vector=(1.0, 0.9, 0.7), noise_id="biome",
        desc="Subtle mineral staining and variety for dry soil and base rock foundations."
    ), "rock": SurfaceModifierProfile(
        intensity=18.0, shift_vector=(0.9, 0.9, 0.9), noise_id="biome",
        desc="Neutral grey and mineral mottle for exposed geologic and rocky features."
    ), "glacier": SurfaceModifierProfile(
        intensity=6.0, shift_vector=(-0.5, 0.1, 1.0), noise_id="biome",
        desc="Deep blue and cool-white variation for permanent ice and glacial features."
    ), "volcanic": SurfaceModifierProfile(
        intensity=20.0, shift_vector=(1.0, 0.4, 0.2), noise_id="biome",
        desc="Aggressive warm and dark shifts for lava flows and volcanic ash deposits."
    ),
}

SURFACE_SPECS: list[SurfaceSpec] = [SurfaceSpec(
    key=SurfaceKey.ARID_BASE,  driver=DriverKey.DEM, coord_factor="elev_m",
    required_factors=("elev_m",), provider_id="ramp", modifiers=[{"id": "mottle", "profile_id": "arid_mottle"}],
    desc="Standard dry-climate soil and rock color ramp."
), SurfaceSpec(
    key=SurfaceKey.ARID_RED_BASE,
    driver=DriverKey.DEM, coord_factor="elev_m", provider_id="ramp",modifiers=[{"id": "mottle", "profile_id": "arid_mottle"}],
    required_factors=("elev_m",),
    desc="Iron-oxide rich (red rock) variant of the arid soil ramp."
), SurfaceSpec(
    key=SurfaceKey.ARID_VEGETATION,  driver=DriverKey.DEM,
    coord_factor="elev_m", provider_id="ramp", required_factors=("elev_m",),
    desc="Dry-climate vegetation colors (sagebrush, scrub, dormant grasses)."
), SurfaceSpec(
    key=SurfaceKey.HUMID_BASE,  driver=DriverKey.DEM, coord_factor="elev_m",
    provider_id="ramp", required_factors=("elev_m",),
    desc="Moist-climate forest floor and damp earth color ramp."
), SurfaceSpec(
    key=SurfaceKey.HUMID_VEGETATION,  driver=DriverKey.DEM,
    coord_factor="elev_m", provider_id="ramp", required_factors=("elev_m",),
    modifiers=[{"id": "mottle", "profile_id": "forest_mottle"}],
    desc="Lush, chlorophyll-rich vegetation colors (conifers, rainforest, meadows)."
), SurfaceSpec(
    key=SurfaceKey.SNOW,  driver=DriverKey.DEM,
    required_factors=("elev_m",), coord_factor="elev_m", provider_id="ramp",
    desc="High-altitude snow and ice color ramp."
), SurfaceSpec(
    key=SurfaceKey.THEME_OVERLAY,
    coord_factor=None, provider_id="style", driver=DriverKey.THEME, required_factors=("theme",),
    modifiers=[{"id": "mottle", "profile_id": "water"}],
    files=frozenset({FileKey.THEME_QML}),
    desc="Categorical colors for specific features (water, rock, glacier) defined in QML."
), ]
