import colorsys
import math
import os
from pathlib import Path
import re
from typing import List, Tuple, Any, Final

import yaml


def new_color_ramp(
        color_ramp_file: str, output_path: str, saturation_multiplier: float, shadow_adjust: float,
        mid_adjust: float, highlight_adjust: float, min_hue: float, max_hue: float,
        target_hue: float, elev_adjust: float
) -> List:
    """
    Reads the color definitions from a color ramp file and updates
    saturation,  hues, or  brightness and writes file (optional). This allows you to have one master
    color-relief file and create variants that align with the overall elevation and coloration of
    the original.
    1) Reads a color ramp file (gdaldem color-relief format)
    2) Adjusts its colors in HSV space using provided parameters
    3) Scales its elev values
    4) Writes a new file (Optional)
    5) Returns the new color ramp table

    Args:
        color_ramp_file: Path to the source GDAL color configuration file.
        output_path: Path where the adjusted configuration file will be saved.
        saturation_multiplier: Multiplies the saturation of each color.
            - 1.0 = no change.
            - > 1.0 = increases saturation (more vivid colors).
            - < 1.0 = decreases saturation (more muted colors).
        shadow_adjust: Additively adjusts the brightness of dark colors (v < 0.5).
        mid_adjust: Additively adjusts the brightness of mid-range colors (v ~ 0.5).
        highlight_adjust: Additively adjusts the brightness of light colors (v > 0.5).
        min_hue: The lower bound (in degrees, 0-360) of the hue range to be adjusted.
        max_hue: The upper bound (in degrees, 0-360) of the hue range to be adjusted.
        target_hue: The target hue (in degrees, 0-360) that colors within the specified
            range will be shifted towards.
        elev_adjust: adjusts all the elev values by the scale provided.  1.0 is no change.
    """
    # Read and parse the GDAL color definition file into a structured table.
    color_table = read_color_ramp(color_ramp_file)

    #  Apply HSV shifts to the color table.
    adjusted_table = hsv_shift_colors(
        color_table, saturation_multiplier=saturation_multiplier, shadow_adjust=shadow_adjust,
        mid_adjust=mid_adjust, highlight_adjust=highlight_adjust, min_hue=min_hue, max_hue=max_hue,
        target_hue=target_hue
    )

    #  Apply the elevation scaling.
    adjusted_table = adjust_elevation(adjusted_table, elev_adjust)

    #  Format and write the adjusted table back to a color ramp file (optional)
    if output_path:
        write_color_file(output_path, adjusted_table)
    return adjusted_table


def adjust_elevation(color_table: list, elev_adjust: float) -> list:
    """
    Scales the elevation value for each data row in a color table.

    This function iterates through the structured color table. For lines that
    contain color data, it multiplies the elevation value by the provided
    'elev_adjust'. Non-data lines (like comments) are passed through unchanged.

    Args:
        color_table: A list of tuples, where each tuple represents a line from
            a GDAL color file, e.g., (True, (elev, r, g, b, a)) for a data line,
            or (False, "# a comment") for a non-data line.
        elev_adjust: A multiplier to apply to each elevation value.
            - 1.0 results in no change.
            - > 1.0 increases the elevation values.
            - < 1.0 decreases the elevation values.

    Returns:
        A new list with the adjusted elevation data, preserving non-data lines.
    """
    if elev_adjust == 1.0:
        return color_table

    adjusted_table = []
    for has_data, data in color_table:
        if not has_data:
            # Pass through non-data lines (comments, 'nv' lines) unchanged
            adjusted_table.append((has_data, data))
            continue

        # Unpack the data, apply the scale, and repack it
        elev, r, g, b, alpha = data
        adjusted_elev = elev * elev_adjust

        adjusted_data = (adjusted_elev, r, g, b, alpha)
        adjusted_table.append((True, adjusted_data))

    return adjusted_table


def hsv_shift_colors(
        color_table: list, saturation_multiplier: float = 1.0, shadow_adjust: float = 0.0,
        mid_adjust: float = 0.0, highlight_adjust: float = 0.0, min_hue: float = 0.0,
        max_hue: float = 0.0, target_hue: float = 0.0
) -> list:
    """
    Applies HSV transformations to a structured list of color data.

    This function iterates through a table of parsed color ramp lines. For each line
    containing color data, it converts the RGB values to HSV, applies all specified
    adjustments, converts the result back to RGB, and stores it. Non-color lines
    (like comments) are preserved in their original form.

    Args:
        color_table: A list of tuples, where each tuple represents a line from
            a GDAL color file, e.g., (True, (elev, r, g, b, a)) for a data line,
            or (False, "# a comment") for a non-data line.
        saturation_multiplier: Multiplies the saturation of each color.
            - 1.0 = no change.
            - > 1.0 = increases saturation.
            - < 1.0 = decreases saturation.
        shadow_adjust: Additively adjusts the brightness of dark colors.
        mid_adjust: Additively adjusts the brightness of mid-range colors.
        highlight_adjust: Additively adjusts the brightness of light colors.
        min_hue: The lower bound (degrees, 0-360) of the hue range to adjust.
        max_hue: The upper bound (degrees, 0-360) of the hue range to adjust.
        target_hue: The target hue (degrees, 0-360) to shift towards.

    Returns:
        A new list with the adjusted color data, preserving non-data lines.
    """
    adjusted_table = []

    def clamp(x):
        """Helper to clamp a value between 0 and 255 and convert to int."""
        return max(0, min(255, int(round(x))))

    for has_data, data in color_table:
        # Pass through non-data lines (comments, 'nv' lines) unchanged
        if not has_data:
            adjusted_table.append((has_data, data))
            continue

        # Unpack, process, and repack data lines
        elev, r, g, b, alpha = data

        # Convert RGB to HSV
        h, s, v = colorsys.rgb_to_hsv(r / 255.0, g / 255.0, b / 255.0)

        # Apply the combined HSV adjustment logic
        h, s, v = adjust_hsv(
            h, s, v, saturation_multiplier=saturation_multiplier, shadow_adjust=shadow_adjust,
            mid_adjust=mid_adjust, highlight_adjust=highlight_adjust, min_hue=min_hue,
            max_hue=max_hue, target_hue=target_hue
        )

        # Convert back to RGB
        r_new, g_new, b_new = [clamp(c * 255.0) for c in colorsys.hsv_to_rgb(h, s, v)]

        # Append the new data to the results table
        adjusted_data = (elev, r_new, g_new, b_new, alpha)
        adjusted_table.append((True, adjusted_data))

    return adjusted_table


def adjust_hsv(
        h: float, s: float, v: float, saturation_multiplier: float, shadow_adjust: float,
        mid_adjust: float, highlight_adjust: float, min_hue: float, max_hue: float,
        target_hue: float
) -> tuple[float, float, float]:
    """Adjusts Hue, Saturation, and Value (HSV) for a single color with advanced controls.

    This function goes beyond a simple linear adjustment. It uses a weighted
    approach to modify brightness and is carefully designed to prevent the
    unintended colorization of neutral tones (greys, whites, blacks) when
    shifting hues.

    The key adjustment steps are:

    1.  **Brightness (Value):** The brightness adjustment is not uniform. It is
        applied as a weighted blend based on the color's original brightness,
        allowing for separate, targeted control over shadows, mid-tones, and
        highlights.
    2.  **Hue:**
        The hue shift is moderated by two factors:
        A **"fade factor"** based on the color's original saturation and
        brightness, which protects neutral tones (greys, whites, blacks).
        A **"range weight"** that creates a smooth falloff, applying the
        strongest hue shift to colors at the center of the `min/max-hue`
        range and a weaker shift to colors near the edges.
        This handles wrapping around the 360-degree color wheel (e.g., a range from 330 to 30) and
        will shift in the shorter direction.
    3.  **Saturation:** The saturation is adjusted with a direct multiplier.

    All final HSV values are clamped to the valid [0.0, 1.0] range.

    Args:
        h (float): The original Hue (0.0 to 1.0).
        s (float): The original Saturation (0.0 to 1.0).
        v (float): The original Value/Brightness (0.0 to 1.0).
        saturation_multiplier (float): Multiplies the saturation. `> 1.0`
            increases vividness, `< 1.0` desaturates.
        shadow_adjust (float): Additively adjusts the brightness of dark colors.
        mid_adjust (float): Additively adjusts the brightness of mid-range colors.
        highlight_adjust (float): Additively adjusts the brightness of light colors.
        min_hue (float): The lower bound of the hue range to adjust (0-360).
        max_hue (float): The upper bound of the hue range to adjust (0-360).
        target_hue (float): The target hue that colors in the range will be
            shifted towards (0-360).

    Returns:
        A tuple containing the new (h, s, v) values, each clamped between 0.0 and 1.0.
    """
    # --- Calculate Final Brightness (Value) ---
    # Create weights based on the original brightness (v). A color can be a mix
    # of shadow/mid or mid/highlight.
    shadow_weight = max(0, 1 - v * 2)
    highlight_weight = max(0, (v - 0.5) * 2)
    mid_weight = 1 - shadow_weight - highlight_weight  # Or 1 - abs((v-0.5)*2)

    total_adjustment = (
            shadow_weight * shadow_adjust + mid_weight * mid_adjust + highlight_weight *
            highlight_adjust)
    final_v = max(0.0, min(1.0, v + total_adjustment))

    # --- Calculate Fade Factor to Protect Greys ---
    # This factor approaches 0 for very dark, very bright, or desaturated colors.
    # It prevents the hue shift from "polluting" neutral tones with color.
    fade_factor = (1 - abs((v - 0.5) * 2)) * min(1.0, s * 4)

    # ---  Calculate and Apply Hue Shift with Falloff ---
    final_h = h
    min_h_norm, max_h_norm, target_h_norm = min_hue / 360.0, max_hue / 360.0, target_hue / 360.0

    in_range = (min_h_norm <= h <= max_h_norm) if min_h_norm <= max_h_norm else (
            h >= min_h_norm or h <= max_h_norm)

    if in_range and min_hue != max_hue:
        # --- Support SMOOTH FALLOFF ---
        #  Calculate the center and width of the hue range, handling wrap-around.
        if min_h_norm <= max_h_norm:
            range_width = max_h_norm - min_h_norm
            range_center = min_h_norm + (range_width / 2)
        else:  # Handle wrap-around case (e.g., 330 to 30 degrees)
            range_width = (1.0 - min_h_norm) + max_h_norm
            range_center = (min_h_norm + range_width / 2) % 1.0

        #  Calculate the color's distance from the center of the range.
        dist_from_center = abs(h - range_center)
        # Handle wrap-around distance
        if dist_from_center > 0.5:
            dist_from_center = 1.0 - dist_from_center

        #  Create a weight that is 1.0 at the center and falls to 0.0 at the edges.
        #    We use a cosine curve for a very smooth, natural falloff.
        if range_width > 0:
            normalized_dist = dist_from_center / (range_width / 2)
            # The cosine function creates a perfect bell-curve like falloff.
            range_weight = math.cos(normalized_dist * (math.pi / 2))
        else:  # Avoid division by zero if range_width is 0
            range_weight = 1.0

        # Calculate the shortest distance around the color wheel.
        diff = target_h_norm - h
        if diff > 0.5:
            diff -= 1.0
        elif diff < -0.5:
            diff += 1.0

        # Apply the change, now moderated by BOTH the fade_factor and the new range_weight.
        hue_change = diff * fade_factor * range_weight
        final_h = (h + hue_change) % 1.0

    # --- Calculate and Apply Saturation Change ---
    final_s = s * saturation_multiplier
    final_s = max(0.0, min(1.0, final_s))

    return final_h, final_s, final_v


def get_ramp_from_yml(
        ramp_name: str, ramps_yml_settings: str, base_ramp: str, output_path: str
):
    """
    Reads YAML file to get specs for the named ramp.  This supports 3 modes:
    1. file - reads the filename configured
    2. color - creates a ramp with the configured color
    3. hsv - Generates a new color ramp by applying configured HSV shift factors

    Sample YAML:
    RAMPS:
        humid_color_ramp:
          mode: hsv
          saturation: 1.0
          shadow-adjust: 0
          mid-adjust: 0
          highlight-adjust: 0
          min-hue: 0
          max-hue: 0
          target-hue: 0
          elev-adjust: 1.0
        snow_color_ramp:
           mode: color
           color: EF3E15
        arid_color_ramp:
          mode: file
          file: arid_color.txt

    Args:
        ramp_name: The key in the YAML file to use (e.g. 'humid_color_ramp').
        ramps_yml_settings: Path to the YAML file containing ramp  definitions.
        base_ramp: Path to the master color text file to be adjusted.
        output_path: Path where the new color file will be saved. No output if None.

    Returns:
        The color table filename for named ramp

    Raises:
        FileNotFoundError: If the YAML file or Base Input file does not exist.
        ValueError: If the ramp_name is not found in the YAML.
    """

    # Validate Inputs

    # Read in  settings from YML config file for this ramp name
    mode, yml_config = read_yml_settings(ramps_yml_settings, ramp_name)
    context = f"Get RAMPS YML entry: {ramp_name} Mode: {mode}\n"

    # Call the HSV Adjuster
    # Note: YAML keys use hyphens (mid-adjust), function args use underscores (mid_adjust)
    if mode == "dynamic":
        return mode, None
    elif mode == "hsv":
        print(f"HSV mode. {context}")
        if not os.path.exists(base_ramp):
            raise FileNotFoundError(f"Base color ramp file not found: {base_ramp}\n{context}")

        new_color_ramp(
            color_ramp_file=base_ramp, output_path=output_path,
            saturation_multiplier=float(yml_config["saturation"]),
            shadow_adjust=float(yml_config["shadow-adjust"]),
            mid_adjust=float(yml_config["mid-adjust"]),
            highlight_adjust=float(yml_config["highlight-adjust"]),
            min_hue=float(yml_config["min-hue"]), max_hue=float(yml_config["max-hue"]),
            target_hue=float(yml_config["target-hue"]), elev_adjust=float(yml_config["elev-adjust"])
        )
        return mode, output_path
    elif mode == "color":
        # get rgb_color and create file
        create_rgb_ramp(output_path, yml_config["color"])
        return mode, output_path
    elif mode == "file":
        # return filename
        if "file" not in yml_config:
            raise ValueError(
                f"Yml config item {ramp_name} has mode 'file' but no file entry.\n{context}"
            )

        return mode, yml_config["file"]
    else:
        return None, None


def read_yml_settings(ramps_config: str | Path, ramp_name: str) -> tuple[str, dict[str, Any]]:
    """Read a ramp entry from a ramps YAML file and normalize it.

    The YAML file is expected to contain a top-level mapping key ``RAMPS`` whose values
    are per-ramp configuration mappings. Each ramp entry supports **one of these**
    mutually exclusive modes:

    1) **Solid-color definition** 
       - Provide a non-empty ``color`` key (a 6-digit RGB hex string, optionally prefixed
         with ``#``).
       - In this mode, the configuration is returned **as-is** and no HSV defaults are
         injected.

    2) **HSV-shift definition** (used to derive a ramp from a base ramp)
       - Omit ``color``
       - In this mode, missing HSV-related fields are filled with defaults so downstream
         code can rely on a complete set of keys.

    3) **File Name** - Reads the specified Color Text File



    Args:
        ramps_config: Path to the ramps YAML configuration file.
        ramp_name: Ramp key/name to read under the ``RAMPS`` root mapping.

    Returns a tuple ``(mode, config)``:
      - ``mode`` - hsv, color, file
      - yml config dict

    Raises:
        FileNotFoundError: If ``ramps_hsv_settings`` does not exist.
        ValueError: If the YAML cannot be parsed, if ``RAMPS`` is missing or not a
            mapping, if ``ramp_name`` is not present, or if the ramp entry is not
            a mapping.
    """

    if not os.path.exists(ramps_config):
        raise FileNotFoundError(f"Ramp configuration file not found: {ramps_config}")

    # Parse YAML
    try:
        with open(ramps_config, "r") as f:
            all_ramps = yaml.safe_load(f)
    except yaml.YAMLError as e:
        raise ValueError(f"Failed to parse ramp YAML file: {e}")

    # Access the 'RAMPS' root key
    config_root = (all_ramps or {}).get("RAMPS")
    if not isinstance(config_root, dict):
        raise ValueError(
            f"Ramp YAML must contain a mapping at root key 'RAMPS'. Got: "
            f"{type(config_root).__name__}"
        )

    if ramp_name not in config_root:
        available = ", ".join(config_root.keys())
        raise ValueError(
            f"Ramp '{ramp_name}' not found in {ramps_config}.\n Available: {available}"
        )

    ramp_item = config_root[ramp_name] or {}
    if not isinstance(ramp_item, dict):
        raise ValueError(
            f"Ramp '{ramp_name}' config must be a mapping, got: {type(ramp_item).__name__}"
        )

    # -------------------------
    # Modes supported: 1) "hsv" HSV definitions, 2) "color" solid-color definition, 3) "file"
    # ---------------------------
    mode = ramp_item["mode"]

    # Solid-color definition.  Return as-is
    if mode == "color":
        if ramp_item["color"] in (None, ""):
            raise ValueError(f"⚠️  Invalid Color definition for {ramp_name}")
        return mode, dict(ramp_item)
    elif mode == "file":
        return mode, dict(ramp_item)
    elif mode == "dynamic":
        return mode, None
    elif mode == "hsv":
        # HSV Definition - inject HSV defaults for any missing HSV fields.
        hsv_defaults = {
            "saturation": 1.0, "shadow-adjust": 0.0, "mid-adjust": 0.0, "highlight-adjust": 0.0,
            "min-hue": 0.0, "max-hue": 0.0, "target-hue": 0.0, "elev-adjust": 1.0,
        }
        return mode, {**hsv_defaults, **ramp_item}
    else:
        raise ValueError(f"⚠️  Unknown ramp mode {mode}")


def parse_gdal_line(line: str) -> tuple[bool, any]:
    """
    Parses a single line from a GDAL color ramp file.

    It distinguishes between data lines (containing elevation and color values)
    and non-data lines (like comments or the 'nv' keyword for no-data).

    Args:
        line: A single, stripped line from the GDAL color text file.

    Returns:
        A tuple of `(has_data, data)` where:
        - `has_data` (bool): Is True if the line contains color data, False otherwise.
        - `data`: If `has_data` is True, this is a tuple of
          (elevation, r, g, b, alpha). Alpha may be None.
          If `has_data` is False, this is the original string of the line.

    Raises:
        ValueError: If a data line has an invalid format or its color
            values are outside the valid 0-255 range.
    """
    line = line.strip()
    if line.startswith("#") or line.startswith("nv"):
        return False, line

    # Split the line using comma, tab, or space as separators
    parts = re.split(r'[,\t\s]+', line)

    if not 4 <= len(parts) <= 5:
        raise ValueError(
            f"Invalid line format: expected 4 or 5 values, but got {len(parts)} in '{line}'"
        )

    try:
        elevation = float(parts[0]) if '.' in parts[0] else int(parts[0])
        color_values = [int(value) for value in parts[1:]]
    except (ValueError, IndexError):
        raise ValueError(
            "Elevation must be a number and color values must be integers."
        )

    if not all(0 <= value <= 255 for value in color_values):
        raise ValueError("Color values must be between 0 and 255.")

    r, g, b, *a = color_values
    alpha = a[0] if a else None

    return True, (elevation, r, g, b, alpha)


def read_color_ramp(input_path: str) -> List[Tuple[bool, Any]]:
    """
    Reads and parses a GDAL color ramp file into a structured list.

    Args:
        input_path: The path to the source GDAL color configuration file.

    Returns:
        A list of tuples, where each tuple represents a parsed line from the file.
        For data lines: (True, (elevation, r, g, b, alpha))
        For non-data lines: (False, "original line content")

    Raises:
        FileNotFoundError: If the input file does not exist.
    """
    color_table = []
    input_file = Path(input_path)
    if not input_file.exists():
        raise FileNotFoundError(f"Ramp file not found: {input_path}")

    with input_file.open("r", encoding="utf-8") as f:
        for line in f:
            if not line.strip():
                continue
            has_data, data = parse_gdal_line(line)
            color_table.append((has_data, data))

    return color_table


def write_color_file(output_path: str, color_table: List[Tuple[bool, Any]]) -> List:
    """
    Writes a structured color table to a GDAL color ramp file.

    Args:
        output_path: The path where the configuration file will be saved.
        color_table: A structured list representing the color ramp data,
            as produced by `read_color_file`.

    Raises:
        IOError: If the file cannot be written.
    """
    output_lines = []
    for has_data, data in color_table:
        if has_data:
            elev, r, g, b, alpha = data
            output_line = f"{elev} {r} {g} {b}"
            if alpha is not None:
                output_line += f" {alpha}"
            output_lines.append(output_line)
        else:
            # For comments or other line types, data is the original string
            output_lines.append(str(data))

    try:
        with open(output_path, "w", encoding="utf-8") as f_out:
            f_out.write("\n".join(output_lines))
    except IOError as e:
        raise IOError(f"Failed to write to output file: {output_path}") from e
    return output_lines


_RGB_HEX_RE: Final[re.Pattern[str]] = re.compile(r"^[0-9a-fA-F]{6}$")

_RAMP_MIN_Z: Final[int] = -100
_RAMP_MAX_Z: Final[int] = 10000
_RAMP_ALPHA: Final[int] = 255


def parse_rgb(hex_color: str):
    """Parse an RGB hex color string into [R, G, B].

    Args:
        hex_color: RGB hex string like "EDEFF0" or "#EDEFF0".

    Returns:
        List of three ints [R, G, B], each in [0, 255].

    Raises:
        ValueError: If `hex_color` is not a valid 6-digit RGB hex string.
    """
    s = hex_color.strip()
    if s.startswith("#"):
        s = s[1:]

    if not _RGB_HEX_RE.fullmatch(s):
        raise ValueError(
            "hex_color must be a 6-digit RGB hex string like 'EDEFF0' (optionally prefixed '#'); "
            f"got: {hex_color!r}"
        )

    r = int(s[0:2], 16)
    g = int(s[2:4], 16)
    b = int(s[4:6], 16)
    return r, g, b


def create_rgb_ramp(filename: str | Path, hex_color: str) -> List:
    """Create a constant-color GDAL color-relief ramp file from an RGB hex string.

    The output ramp contains two stops with identical RGBA values:

        -10000 R G B 255
         10000 R G B 255

    Args:
        filename: Output ramp file path to write.
        hex_color: RGB hex string like "EDEFF0" or "#EDEFF0".

    Returns:
        The written ramp file contents as a list.

    Raises:
        ValueError: If `hex_color` is invalid.
        OSError: If the file cannot be written.
    """
    r, g, b = parse_rgb(hex_color)
    out_path = Path(filename).expanduser().resolve()
    output_lines = [f"{_RAMP_MIN_Z} {r} {g} {b} {_RAMP_ALPHA}\n",
                    f"{_RAMP_MAX_Z} {r} {g} {b} {_RAMP_ALPHA}\n"]

    out_path.write_text("\n".join(output_lines), encoding="utf-8")
    return output_lines
