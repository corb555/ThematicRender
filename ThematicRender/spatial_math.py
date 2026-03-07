import numpy as np
from scipy.ndimage import gaussian_filter, median_filter, binary_fill_holes

EPS = 1e-9

def refine_organic_signal(mask, blur_px, noise_amp, noise_id, contrast, max_opacity, ctx, name):
    """
    Conditions a raw spatial signal into an organic factor.
    Handles: edge smoothing, adds internal noise swings, and contrast sharpening.
    """
    res = np.squeeze(mask).astype(np.float32)

    # 1. Edge Smoothing
    if blur_px > 0:
        res = gaussian_filter(res, sigma=blur_px)

    # 2. Internal Swings
    if noise_id:
        noise_provider = ctx.noises.get(noise_id)
        offset = hash(name) % 1000
        noise = np.squeeze(
            noise_provider.window_noise(ctx.window, row_off=offset, col_off=offset)
        )

        # Add a subtle blur to the noise itself to kill speckling
        # noise = gaussian_filter(noise, sigma=2.0)

        variation = (1.0 - noise_amp) + (noise * noise_amp)
        res = res * variation

    # 3. Sharpening
    if contrast != 1.0:
        res = np.clip((res - 0.5) * contrast + 0.5, 0.0, 1.0)

    # 4.  Final Clip and Opacity
    return np.clip(res, 0.0, 1.0) * max_opacity


def lerp(a, b, t):
    """Safe lerp that prevents (H,W) + (H,W,1) -> (H,W,H) broadcasting."""
    # If t is 3D and b is 2D, expand b
    if hasattr(t, 'ndim') and t.ndim == 3:
        if hasattr(b, 'ndim') and b.ndim == 2:
            b = b[..., np.newaxis]
        if hasattr(a, 'ndim') and a.ndim == 2:
            a = a[..., np.newaxis]

    return a + t * (b - a)


def normalize_step(val: np.ndarray, min_v: float, max_v: float) -> np.ndarray:
    denom = max_v - min_v
    denom = denom if abs(denom) > EPS else 1.0
    fac = (val - min_v) / denom
    return np.clip(fac, 0.0, 1.0)[..., np.newaxis]

def smoothstep(t: np.ndarray) -> np.ndarray:
    t = np.clip(t, 0.0, 1.0)
    return t * t * (3.0 - 2.0 * t)

def get_smooth_theme(theme_2d, label_to_val, smoothing_profiles):
    """
    Rounds corners and expands IDs into 0-space using precedence-based melting.
    100% 2D Contract Safe.
    """
    # 1. Initial cleanup: removes tiny single-pixel noise/speckles
    theme = median_filter(theme_2d, size=3)
    present_ids = np.unique(theme)

    # 2. Setup the "Result" buffer (start with the raw median-filtered version)
    smoothed = theme.copy()
    void_mask = (theme == 0)

    # 3. Order categories by precedence (lower precedence first so higher can overwrite)
    all_labels = list(label_to_val.keys())

    def get_prof(lbl):
        return smoothing_profiles.get(lbl, smoothing_profiles["_default_"])

    order = sorted(all_labels, key=lambda l: get_prof(l).precedence)

    for label in order:
        val = label_to_val.get(label)
        if val not in present_ids or val == 0:
            continue

        prof = get_prof(label)

        # Binary Mask for this specific category (e.g., Playa)
        mask = (theme == val)
        mask = binary_fill_holes(mask)  # Kill small gaps inside the shape

        # THE MELT: Blur the binary mask to create a probability slope
        melted = gaussian_filter(mask.astype(np.float32), sigma=prof.smoothing_radius)

        # THE CLAIM: Define which pixels this category is "strong" enough to take
        # Logic: (Melt value > weight) AND (is currently background OR is a lower-precedence
        # item)
        can_overwrite = np.zeros_like(void_mask, dtype=bool)
        for other_label in all_labels:
            other_val = label_to_val.get(other_label)
            if other_val is None or other_val == val: continue
            # Higher precedence wins the pixel
            if get_prof(other_label).precedence < prof.precedence:
                can_overwrite |= (smoothed == other_val)

        # Update the smoothed ID map
        grow_mask = (melted > prof.expansion_weight) & (void_mask | can_overwrite)
        smoothed[grow_mask] = val

    return smoothed
