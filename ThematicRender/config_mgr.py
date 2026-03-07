from __future__ import annotations

import dataclasses
# config_mgr.py
from dataclasses import dataclass
from pathlib import Path
from typing import (Any, Tuple, Iterable, Set, Dict)

import numpy as np
from YMLEditor.yaml_reader import ConfigLoader

from ThematicRender.compositing_library import COMPOSITING_REGISTRY
from ThematicRender.keys import DriverKey, FileKey, NoiseProfile, RequiredResources, _BlendSpec, \
    SurfaceSpec, FactorSpec, PipelineRequirements
from ThematicRender.schema import RENDER_SCHEMA
from ThematicRender.settings import NOISE_PROFILES, SURFACE_MODIFIER_PROFILES
from ThematicRender.utils import DTYPE_ALIASES, GenMarkdown


# config_mgr.py
@dataclass(slots=True)
class ConfigMgr:
    """
    The Single Source of Truth for a build.
    All merging and path resolution is done at construction (build time).
    """
    logic: Dict[str, Any]  # Fused DRIVER_LOGIC_PARAMS
    specs: Dict[DriverKey, Any]  # Fused DRIVER_SPECS (DriverSpec objects)
    files: Dict[str, Path]  # Resolved absolute Paths
    raw_defs: Dict[str, Any]  # Top-level project settings (seed, etc.)

    @classmethod
    def build(cls, config_path: Path, prefix: str = "", output_override: str = None) -> "ConfigMgr":
        """
        The factory that 'Fuses' settings.py and YAML into a single store.
        """
        # 1. Load project YAML
        loader = ConfigLoader(RENDER_SCHEMA)
        defs = loader.read(config_file=config_path)

        # 2. Fuse Logic Parameters (Defaults from settings.py + Project Overrides)
        from ThematicRender.settings import DRIVER_LOGIC_PARAMS
        fused_logic = {}
        # Start with a deep copy of defaults
        for k, v in DRIVER_LOGIC_PARAMS.items():
            fused_logic[k] = dict(v)

        # Overlay YAML overrides (e.g. YAML 'drivers' block)
        yaml_drivers = defs.get("drivers", {})
        for key, params in yaml_drivers.items():
            if key in fused_logic:
                fused_logic[key].update(params)
            else:
                fused_logic[key] = params

        # 3. Fuse Driver Hardware Specs
        from ThematicRender.settings import DRIVER_SPECS
        fused_specs = {}
        yaml_specs = defs.get("driver_specs", {})
        for dkey in DriverKey:
            base = DRIVER_SPECS.get(dkey)
            # Update specific fields if YAML provides them (halo, cleanup_type, etc)
            override = yaml_specs.get(dkey.value, {})
            fused_specs[dkey] = dataclasses.replace(base, **override) if override else base

        # 4. Resolve Paths
        resolved_files = {}
        static_files = defs.get("files", {})
        prefixed_files = defs.get("prefixed_files", {})

        # Combine and expand paths
        for k, v in static_files.items():
            resolved_files[k] = Path(v).expanduser()
        for k, v in prefixed_files.items():
            resolved_files[k] = Path(f"{prefix}{v}").expanduser()

        # Set Final Output
        out_path = output_override or defs.get("output", "output.tif")
        resolved_files["output"] = Path(out_path).expanduser()

        return cls(
            logic=fused_logic, specs=fused_specs, files=resolved_files, raw_defs=defs
        )

    # --- Standard Accessors ---

    def get_logic(self, key: str) -> Dict[str, Any]:
        """Returns math params (start, full, noise_amp)."""
        return self.logic.get(key, {})

    def get_spec(self, key: DriverKey) -> Any:
        """Returns hardware/storage specs (halo, cleanup_type)."""
        return self.specs.get(key)

    def path(self, key: str) -> Path:
        """Returns the absolute Path for a file key."""
        p = self.files.get(key)
        if not p:
            # Note: We let the app handle the failure if a file is missing
            return None
        return p

    def get_global(self, key: str, default: Any = None) -> Any:
        """Access top-level project settings like 'seed'."""
        return self.raw_defs.get(key, default)


def derive_pipeline_requirements(
        pipeline: Iterable[_BlendSpec], surface_specs: Iterable[SurfaceSpec],
        factor_specs: Iterable[FactorSpec]
) -> PipelineRequirements:
    """
    Scans the pipeline recursively to find all required Factors and Surfaces.
    """
    req_factors: Set[str] = set()
    req_surfaces: Set[Any] = set()
    produced_surfaces: Set[Any] = set()

    ss_lookup = {ss.key: ss for ss in surface_specs}
    fs_lookup = {fs.name: fs for fs in factor_specs}

    active_steps = [s for s in pipeline if s.enabled]

    # PASS 1: Pipeline direct needs
    for step in active_steps:
        if step.factor_nm: req_factors.add(step.factor_nm)
        if step.output_surface: produced_surfaces.add(step.output_surface)
        if step.input_surfaces:
            for skey in step.input_surfaces: req_surfaces.add(skey)

    # PASS 2: Recursive Dependency Discovery
    processed_surfaces = set()
    processed_factors = set()

    while True:
        new_surfaces = req_surfaces - processed_surfaces
        new_factors = req_factors - processed_factors
        if not new_surfaces and not new_factors: break

        for skey in new_surfaces:
            spec = ss_lookup.get(skey)
            if spec:
                if spec.coord_factor: req_factors.add(spec.coord_factor)
                for f_req in spec.required_factors: req_factors.add(f_req)
            processed_surfaces.add(skey)

        for fname in new_factors:
            spec = fs_lookup.get(fname)
            if spec and spec.required_factors:
                for f_req in spec.required_factors: req_factors.add(f_req)
            processed_factors.add(fname)

    return PipelineRequirements(
        factor_names=req_factors, surface_inputs=req_surfaces - produced_surfaces
    )


Slice2D = Tuple[slice, slice]


def _parse_dtype(v: Any, *, where: str) -> np.dtype:
    """Parse dtype from config values."""
    if v is None:
        raise ValueError(f"{where}: dtype is None")

    if isinstance(v, np.dtype):
        return v

    if isinstance(v, type) and issubclass(v, np.generic):
        return np.dtype(v)

    if isinstance(v, str):
        key = v.strip().lower()
        if key in DTYPE_ALIASES:
            return np.dtype(DTYPE_ALIASES[key])
        raise ValueError(f"{where}: unknown dtype string '{v}'")

    raise ValueError(f"{where}: unsupported dtype {type(v).__name__}: {v!r}")


def _require_comp_ops(pipeline_list: list[_BlendSpec], required_ops: set[str]) -> None:
    enabled = [s for s in pipeline_list if getattr(s, "enabled", True)]
    enabled_ops = {getattr(s, "comp_op", None) or getattr(s, "action", None) for s in enabled}
    enabled_ops.discard(None)

    missing = required_ops - enabled_ops
    if missing:
        pretty_enabled = [
            f"{i}: comp_op={getattr(s, 'comp_op', None)!r} target={getattr(s, 'target', None)!r}"
            for i, s in enumerate(enabled)]
        raise ValueError(
            "\n❌ PIPELINE CONFIG ERROR\n"
            f"Missing required pipeline steps: {sorted(missing)}\n"
            "Enabled steps:\n  - " + "\n  - ".join(pretty_enabled) + "\n"
                                                                     "Your pipeline must include "
                                                                     "an enabled "
                                                                     "comp_op='create_buffer' step "
                                                                     "before "
                                                                     "comp_op='write_output'.\n"
        )


def derive_resources(
        *, cfg, pipeline: Iterable[_BlendSpec], factor_specs: Iterable[FactorSpec],
        surface_specs: Iterable[SurfaceSpec]
) -> RequiredResources:
    # materialize once so we can safely inspect it later
    pipeline_list = list(pipeline)
    _require_comp_ops(pipeline_list, {"create_buffer", "write_output"})

    # 1. Identify Demand
    preq = derive_pipeline_requirements(pipeline, surface_specs, factor_specs)
    fs_lookup = {fs.name: fs for fs in factor_specs}
    ss_lookup = {ss.key: ss for ss in surface_specs}

    req_drivers: Set[DriverKey] = set()
    req_files: Set[FileKey] = {FileKey.RAMPS_YML}
    requested_noise_ids: Set[str] = set()

    # 2. Gather from Factors
    for name in preq.factor_names:
        fs = fs_lookup.get(name)
        if not fs:
            continue
        req_drivers.update(fs.drivers)
        if fs.required_noise:
            requested_noise_ids.add(fs.required_noise)

    # 3. Gather from Surfaces (Modifier Dependencies)
    for sk in preq.surface_inputs:
        ss = ss_lookup.get(sk)
        if not ss:
            continue
        if ss.driver:
            req_drivers.add(ss.driver)
        if ss.files:
            req_files.update(ss.files)

        if ss.modifiers:
            for mod_cfg in ss.modifiers:
                profile_id = mod_cfg.get("profile_id")
                if not profile_id:
                    continue
                v_profile = SURFACE_MODIFIER_PROFILES.get(profile_id)
                if v_profile is None:
                    available_vars = list(SURFACE_MODIFIER_PROFILES.keys())
                    raise ValueError(
                        f"\n❌ CONFIG ERROR: Surface '{sk.value}' requested modifier profile "
                        f"'{profile_id}', but it doesn't exist in SURFACE_MODIFIER_PROFILES.\n"
                        f"👉 Available IDs: {available_vars}"
                    )
                requested_noise_ids.add(v_profile.noise_id)

    # 4. Fulfill Noise Profiles
    noise_profiles: Dict[str, NoiseProfile] = {}
    for nid in requested_noise_ids:
        profile = NOISE_PROFILES.get(nid)
        if profile:
            noise_profiles[nid] = profile
        else:
            available_noises = list(NOISE_PROFILES.keys())
            raise ValueError(
                f"\n❌ FATAL: Pipeline requires noise profile '{nid}', but it's not defined "
                f"in the NOISE_PROFILES table in settings.py.\n"
                f"👉 Ensure the ID matches exactly.\n"
                f"👉 Available Noise IDs: {available_noises}"
            )

    # 6. DETERMINE THE ANCHOR (Geometry)
    explicit_anchor = cfg.get_global("anchor")
    if explicit_anchor:
        anchor_key = DriverKey(explicit_anchor)
    elif DriverKey.DEM in req_drivers:
        anchor_key = DriverKey.DEM
    elif req_drivers:
        anchor_key = sorted(list(req_drivers))[0]
    else:
        print("❌ Error: No drivers found in pipeline. ")
        res = RequiredResources(
            drivers=req_drivers, files=req_files, anchor_key=None, noise_profiles=noise_profiles,
            factor_inputs=preq.factor_names, surface_inputs=preq.surface_inputs,
            primary_surface=None, )
        report = analyze_pipeline(
            cfg=cfg, resources=res, pipeline=pipeline, factor_specs=factor_specs,
            surface_specs=surface_specs
        )
        print(report)
        raise RuntimeError("❌ Error: No drivers found in pipeline. ")

    primary = None
    return RequiredResources(
        drivers=req_drivers, files=req_files, anchor_key=anchor_key, noise_profiles=noise_profiles,
        factor_inputs=preq.factor_names, surface_inputs=preq.surface_inputs,
        primary_surface=primary, )


def analyze_pipeline(cfg: ConfigMgr, resources, pipeline, factor_specs, surface_specs) -> str:
    from ThematicRender.settings import SURFACE_MODIFIER_PROFILES
    md = GenMarkdown()

    warnings = []
    step_with_warnings = set()

    # Helper to add a warning and tag the specific pipeline index
    def add_step_warning(idx, msg):
        warnings.append(msg)
        if isinstance(idx, int):
            step_with_warnings.add(idx)

    # 1. Prepare Lookups
    fs_lookup = {fs.name: fs for fs in factor_specs}
    ss_lookup = {ss.key: ss for ss in surface_specs}

    # 2. Track Simulated State (Sequence Validation)
    sim_buffers = set()
    sim_surfaces = set(resources.surface_inputs)

    # --- 0. PRE-FLIGHT LINTER ---
    for i, step in enumerate(pipeline):
        if not step.enabled:
            continue

        meta = COMPOSITING_REGISTRY.get(step.comp_op)
        if meta is None:
            add_step_warning(i, f"🔴 **Step {i}:** Unknown operation `{step.comp_op}`.")
            continue

        # A. Check Inputs
        if "input_surfaces" in meta.required_attrs:
            for skey in (step.input_surfaces or []):
                if skey not in sim_surfaces:
                    add_step_warning(
                        i,
                        f"⚠️ **Step {i}:** Surface `"
                        f"{skey.value if hasattr(skey, 'value') else skey}` is used before being "
                        f"created."
                        )

        # B. Check Buffers
        if "buffer" in meta.required_attrs and step.comp_op != "create_buffer":
            if step.buffer not in sim_buffers:
                add_step_warning(
                    i,
                    f"🔴 **Step {i}:** Requires buffer `{step.buffer}`, but it hasn't been "
                    f"initialized."
                    )

        if step.comp_op == "lerp_buffers":
            if step.merge_buffer not in sim_buffers:
                add_step_warning(
                    i,
                    f"🔴 **Step {i}:** Merge buffer `{step.merge_buffer}` has not been initialized."
                    )

        # C. Check Spec Attributes
        for attr in meta.required_attrs:
            val = getattr(step, attr, None)
            if val is None or (hasattr(val, "__len__") and len(val) == 0):
                add_step_warning(
                    i, f"🔴 **Step {i} ({step.comp_op}):** Missing required attribute `{attr}`."
                    )

        # D. Check Signal Shaping
        if abs(step.contrast) > 1.5:
            add_step_warning(
                i, f"⚠️ **Step {i}:** High contrast ({step.contrast}) may be clipping signal."
                )
        if step.factor_nm and step.scale < 0.1:
            add_step_warning(i, f"⚠️ **Step {i}:** Factor scale is extremely low ({step.scale}).")

        # E. Update Simulation State
        if step.comp_op == "create_buffer":
            sim_buffers.add(step.buffer)
        if step.output_surface:
            sim_surfaces.add(step.output_surface)

    # --- 1. HEADER & SUMMARY ---
    md.header("Thematic Render: Execution Flow", 1)
    md.bullet(f"{md.bold('Output:')} `{cfg.path('output')}`")

    anchor_key = resources.anchor_key
    if anchor_key:
        md.bullet(f"{md.bold('Anchor:')} `{anchor_key.value}` (Defines master geometry)")
    else:
        add_step_warning("Global", "❌ **No Anchor Key:** Cannot determine geometry.")

    # Warnings Summary Block
    md.header("🚨 Pipeline Warnings", 3)
    if warnings:
        for w in warnings:
            md.bullet(w)
    else:
        md.text("None - Pipeline logic appears sound.")
    md.text("---")

    # --- 2. EXECUTION NARRATIVE ---
    md.header("1. Enabled Pipeline Steps", 2)

    seen_factors = set()
    seen_surfaces = set()

    for i, step in enumerate(pipeline):
        if not step.enabled:
            continue

        # Resolve Target Name
        meta = COMPOSITING_REGISTRY.get(step.comp_op)
        if meta and "output_surface" in meta.required_attrs:
            target = step.output_surface.value if hasattr(step.output_surface, 'value') else str(
                step.output_surface
                )
        else:
            target = step.buffer

        warning_icon = "⚠️ " if i in step_with_warnings else ""
        md.header(f"Step {i}) {warning_icon}{step.desc}", 3)
        md.bullet(f"{md.bold('Operation:')} `{step.comp_op}`")

        if step.comp_op == "lerp_buffers":
            md.bullet(
                f"{md.bold('Logic:')} Blend buffer `{step.merge_buffer}` into `{step.buffer}`"
                )
        else:
            md.bullet(f"{md.bold('Target:')} `{target}`")

        # Factor Details
        if step.factor_nm:
            fname = step.factor_nm
            signal = f"Scale: {step.scale} | Bias: {step.bias} | Contrast: {step.contrast}"

            if fname not in seen_factors:
                seen_factors.add(fname)
                fs = fs_lookup.get(fname)
                if fs:
                    md.bullet(f"{md.bold('Factor:')} `{fname}` (First Sighting)")
                    md.text(
                        f"  * *Logic:* `{fs.function_id}` using `"
                        f"{', '.join([d.value for d in fs.drivers])}`"
                        )
                    if fs.required_noise:
                        md.text(f"  * *Noise Source:* `{fs.required_noise}`")
                else:
                    md.bullet(f"{md.bold('Factor:')} `{fname}` {md.italic('(No Spec Found)')}")
            else:
                md.bullet(f"{md.bold('Factor:')} `{fname}` {md.italic('(Referenced)')}")

            md.text(f"  * *Signal Shaping:* {signal}")

        # Inbound Surface Details
        if step.input_surfaces:
            md.bullet(md.bold("Inbound Surfaces:"))
            for skey in step.input_surfaces:
                if skey not in seen_surfaces:
                    seen_surfaces.add(skey)
                    ss = ss_lookup.get(skey)
                    if ss:
                        mods = ", ".join(
                            [f"{m['id']}({m['profile_id']})" for m in ss.modifiers]
                            ) if ss.modifiers else "None"
                        md.text(f"  * `{skey.value}` (First Sighting)")
                        md.text(f"    * *Provider:* `{ss.provider_id}`")
                        if ss.coord_factor: md.text(f"    * *Sampling Factor:* `{ss.coord_factor}`")
                        md.text(f"    * *Modifiers:* {mods}")
                    else:
                        md.text(f"  * `{skey.value}` {md.italic('(Computed Buffer/Surface)')}")
                else:
                    md.text(f"  * `{skey.value}` {md.italic('(Referenced)')}")

        md.text("")

    # --- 3. RESOURCE APPENDIX ---
    md.header("2. Global Resource Registry", 2)

    # Physical Drivers Table
    md.header("Physical Drivers", 3)
    md.tbl_hdr("Key", "Halo", "Cleanup Logic", "File Path")
    for dkey in sorted(list(resources.drivers)):
        dspec = cfg.get_spec(dkey)
        path = cfg.path(dkey.value)
        cleanup = f"{dspec.cleanup_type} (r={dspec.smoothing_radius})" if dspec.cleanup_type else\
            "None"
        md.tbl_row(f"`{dkey.value}`", f"{dspec.halo_px}px", cleanup, f"`{path}`")

    # Noise Table
    md.header("Noise Profiles", 3)
    md.tbl_hdr("ID", "Sigmas (Scales)", "Weights", "Description")
    for nid, prof in resources.noise_profiles.items():
        md.tbl_row(f"`{nid}`", str(prof.sigmas), str(prof.weights), prof.desc)

    # Modifier Profiles
    md.header("Surface Modifier Profiles", 3)
    md.tbl_hdr("Profile ID", "Intensity", "Shift (RGB)", "Noise Source")
    active_profile_ids = set()
    for skey in resources.surface_inputs:
        ss = ss_lookup.get(skey)
        if ss and ss.modifiers:
            active_profile_ids.update([m["profile_id"] for m in ss.modifiers])

    for pid in sorted(list(active_profile_ids)):
        vprof = SURFACE_MODIFIER_PROFILES.get(pid)
        if vprof:
            md.tbl_row(f"`{pid}`", vprof.intensity, str(vprof.shift_vector), f"`{vprof.noise_id}`")

    return md.render()
