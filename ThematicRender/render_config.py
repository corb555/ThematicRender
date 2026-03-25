from __future__ import annotations

# config_mgr.py
from dataclasses import dataclass
from pathlib import Path
from typing import (Any, Tuple, Iterable, Set, Dict, List, Optional)

import numpy as np
from rasterio.windows import Window
from YMLEditor.yaml_reader import ConfigLoader

from ThematicRender.compositing_library import COMPOSITING_REGISTRY
from ThematicRender.keys import DriverKey, FileKey, NoiseSpec, RequiredResources, _BlendSpec, \
    SurfaceSpec, FactorSpec, PipelineRequirements, SurfaceModifierSpec, SurfaceKey, DriverRndrSpec
from ThematicRender.schema import RENDER_SCHEMA
from ThematicRender.utils import DTYPE_ALIASES, GenMarkdown


# render_config.py

class RenderConfigError(ValueError):
    """Raised when the Render configuration is logically invalid or missing files."""
    pass


@dataclass(slots=True)
class JobManifest:
    job_id: str
    render_cfg: "RenderConfig"
    resources: "RequiredResources"

    final_out_path: Path
    temp_out_path: Path

    profile: dict
    region_id: str
    envelope: Optional[Window]
    write_offset: Tuple[int, int]
    render_params: Tuple[float, float, float]
    driver_metadata: dict[DriverKey, dict[str, int]]


@dataclass(slots=True)
class RenderConfig:
    logic: Dict[str, Any]
    specs: Dict[DriverKey, DriverRndrSpec]
    files: Dict[str, Path]
    raw_defs: Dict[str, Any]
    pipeline: List[_BlendSpec]
    factors: List[FactorSpec]
    surfaces: List[SurfaceSpec]
    noises: Dict[str, NoiseSpec]
    modifiers: Dict[str, SurfaceModifierSpec]
    theme_render: Dict[str, Any]
    theme_smoothing_specs: Dict[str, Any]

    @classmethod
    def load(cls, config_path: Path) -> "RenderConfig":
        if not config_path.exists():
            raise FileNotFoundError(f"Biome config not found at: {config_path}")

        loader = ConfigLoader(RENDER_SCHEMA)
        try:
            defs = loader.read(config_file=config_path)
        except Exception as e:
            raise RenderConfigError(f"YAML Syntax Error in {config_path.name}: {e}")

        print(f"RenderConfig LOADING {config_path}")


        context = "initialization"
        current_item = "n/a"

        try:
            # 1. NOISE PROFILES
            context = "noise_profiles"
            noises = {}
            for nid, data in defs.get("noise_profiles", {}).items():
                current_item = nid
                noises[nid] = NoiseSpec(
                    id=nid, sigmas=tuple(data["sigmas"]), weights=tuple(data["weights"]),
                    stretch=tuple(data.get("stretch", [1.0, 1.0])),
                    seed_offset=int(data.get("seed_offset", 0)), desc=data.get("desc", "")
                )

            # 2. SURFACE MODIFIER SPECS
            context = "surface_modifier_specs"
            modifiers = {}
            for mid, data in defs.get("surface_modifier_specs", {}).items():
                current_item = mid
                modifiers[mid] = SurfaceModifierSpec(
                    intensity=float(data["intensity"]), shift_vector=tuple(data["shift_vector"]),
                    noise_id=data["noise_id"], desc=data.get("desc", "")
                )

            # 3. DRIVER SPECS
            context = "driver_specs"
            driver_specs = {}
            for dname, data in defs.get("driver_specs", {}).items():
                current_item = dname
                dkey = to_enum(DriverKey, dname)
                driver_specs[dkey] = DriverRndrSpec(
                    dtype=data.get("dtype", "float32"), halo_px=int(data.get("halo_px", 64)),
                    cleanup_type=data.get("cleanup_type"),
                    smoothing_radius=float(data.get("smoothing_radius", 0)) if data.get(
                        "smoothing_radius"
                        ) else None
                )

            # 4. FACTORS
            context = "factors"
            factors = []
            yaml_factors = {**defs.get("factors", {}), **defs.get("factor_specs", {})}
            for fname, data in yaml_factors.items():
                current_item = fname
                factors.append(
                    FactorSpec(
                        name=fname, function_id=data["function_id"],
                        drivers=frozenset(to_enum(DriverKey, d) for d in data["drivers"]),
                        required_noise=data.get("required_noise"),
                        required_factors=tuple(data.get("required_factors", [])),
                        params=data.get("params", {}), desc=data.get("desc", "")
                    )
                )

            # 5. SURFACES
            context = "surfaces"
            surfaces = []
            for sname, data in defs.get("surfaces", {}).items():
                current_item = sname
                surfaces.append(
                    SurfaceSpec(
                        key=to_enum(SurfaceKey, sname), driver=to_enum(DriverKey, data["driver"]),
                        coord_factor=data.get("coord_factor"),
                        required_factors=tuple(data.get("required_factors", [])),
                        provider_id=data.get("provider_id", "ramp"),
                        modifiers=data.get("modifiers", []),
                        files=frozenset(to_enum(FileKey, f) for f in data.get("files", [])),
                        desc=data.get("desc", "")
                    )
                )

            # 6. THEMES
            theme_render=defs.get("theme_render", {})
            theme_smoothing_specs=defs.get("theme_smoothing_specs", {})

            # 6. BLEND PIPELINE
            context = "pipeline"
            pipeline = []
            for idx, p_def in enumerate(defs.get("pipeline", [])):
                current_item = f"Step #{idx} ({p_def.get('comp_op', 'unknown')})"
                pipeline.append(
                    _BlendSpec(
                        desc=p_def.get("desc", ""), enabled=bool(p_def.get("enabled", True)),
                        comp_op=p_def["comp_op"], factor_nm=p_def.get("factor_nm"),
                        input_surfaces=[to_enum(SurfaceKey, s) for s in
                                        p_def.get("input_surfaces", [])],
                        output_surface=to_enum(SurfaceKey, p_def.get("output_surface")),
                        buffer=p_def.get("buffer", "canvas"),
                        merge_buffer=p_def.get("merge_buffer"), bias=float(p_def.get("bias", 0.0)),
                        scale=float(p_def.get("scale", 1.0)),
                        contrast=float(p_def.get("contrast", 0.0)), params=p_def.get("params", {}),
                        mask_nm=p_def.get("mask_nm")
                    )
                )

            return cls(
                logic=defs.get("drivers", {}), specs=driver_specs, files={}, raw_defs=defs,
                pipeline=pipeline, factors=factors, surfaces=surfaces, noises=noises,
                modifiers=modifiers, theme_render=theme_render, theme_smoothing_specs=theme_smoothing_specs
            )

        except KeyError as e:
            raise RenderConfigError(
                f"Missing required field in [{context}] -> '{current_item}': {e}"
                )
        except Exception as e:
            raise RenderConfigError(f"Logic error in [{context}] item '{current_item}': {e}")

    def resolve_paths(self, prefix: str, build_dir: Path, output_file: str) -> None:
        """Finalizes file dictionary and validates that all inputs exist on disk."""
        resolved_files = {}
        missing_files = []

        # 1. Standard Files (Direct paths like QML)
        for k, v in self.raw_defs.get("files", {}).items():
            p = Path(v).expanduser()
            if not p.exists():
                missing_files.append(f"Standard File [{k}]: {p}")
            resolved_files[k] = p

        # 2. Prefixed Files (Input TIFFs like _DEM.tif)
        for k, v in self.raw_defs.get("prefixed_files", {}).items():
            # The 'output' key is handled separately in step 3
            if k == "output": continue

            p = (build_dir / f"{prefix}{v}").resolve()
            if not p.exists():
                missing_files.append(f"Input Raster [{k}]: {p}")
            resolved_files[k] = p

        # 3. Output Destination (Must not exist, but parent must)
        out_path = Path(output_file)
        if not out_path.is_absolute():
            out_path = build_dir / out_path
        out_path = out_path.resolve()

        if not out_path.parent.exists():
            try:
                out_path.parent.mkdir(parents=True, exist_ok=True)
            except Exception as e:
                missing_files.append(f"Output Directory (uncreatable): {out_path.parent} - {e}")

        resolved_files["output"] = out_path

        # 4. Final Validation Check
        if missing_files:
            error_msg = "Path Resolution Failed. The following files are missing:\n" + "\n".join(
                missing_files
                )
            raise FileNotFoundError(error_msg)

        self.files = resolved_files

    # ---  Accessors ---
    @staticmethod
    def get_halo_for_driver(driver):
        return 64

    def get_logic(self, key: str) -> Dict[str, Any]:
        """Returns math params (start, full, noise_amp)."""
        return self.logic.get(key, {})

    def get_spec(self, key: DriverKey, default: Any = None) -> DriverRndrSpec:
        """Returns hardware/storage specs (halo, cleanup_type)."""
        return self.specs.get(key, default)

    def get_smoothing_specs(self) -> Dict[str, Any]:
        """
        Returns the dictionary of thematic smoothing rules (precedence, radius, weight).
        """
        # 1. Try to pull from the 'theme_smoothing_specs' block in biome.yml
        return self.raw_defs.get("theme_smoothing_specs")

    def get_max_halo(self) -> int:
        """Return the maximum halo required by any configured driver."""
        halos = [spec.halo_px for spec in self.specs.values() if
                 getattr(spec, "halo_px", None) is not None]
        return max(halos, default=0)

    def path(self, key: str) -> Path:
        """Returns the absolute Path for a file key."""
        p = self.files.get(key)
        if not p:
            # Note:  let the app handle the failure if a file is missing
            return None
        return p

    def get_global(self, key: str, default: Any = None) -> Any:
        """Access top-level project settings like 'seed'."""
        return self.raw_defs.get(key, default)


Slice2D = Tuple[slice, slice]


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


def derive_resources(*, render_cfg: RenderConfig) -> RequiredResources:
    """
    Scans the configuration to identify all required resources
    """
    # 1. Identify Demand
    # We pull the specific pipeline, factors, and surfaces directly from the cfg
    preq = derive_pipeline_requirements(
        pipeline=render_cfg.pipeline, surface_specs=render_cfg.surfaces,
        factor_specs=render_cfg.factors
    )

    # Create lookups from the ConfigMgr's library
    fs_lookup = {fs.name: fs for fs in render_cfg.factors}
    ss_lookup = {ss.key: ss for ss in render_cfg.surfaces}

    req_drivers: Set[DriverKey] = set()
    req_files: Set[FileKey] = {FileKey.RAMPS_YML}
    requested_noise_ids: Set[str] = set()

    # 2. Gather from Factors (Drivers and Noises)
    for name in preq.factor_names:
        fs = fs_lookup.get(name)
        if fs:
            req_drivers.update(fs.drivers)
            if fs.required_noise:
                requested_noise_ids.add(fs.required_noise)

    # 3. Gather from Surfaces (Drivers, Files, and Modifiers)
    for sk in preq.surface_inputs:
        ss = ss_lookup.get(sk)
        if ss:
            if ss.driver: req_drivers.add(ss.driver)
            if ss.files: req_files.update(ss.files)

            if ss.modifiers:
                for mod_cfg in ss.modifiers:
                    profile_id = mod_cfg.get("profile_id")
                    if profile_id:
                        v_profile = render_cfg.modifiers.get(profile_id)
                        if v_profile:
                            requested_noise_ids.add(v_profile.noise_id)
                        else:
                            raise ValueError(f"❌ Modifier '{profile_id}' not found.")

    # 4. Fulfill Noise Profiles
    # Map the noise IDs to actual NoiseSpec objects stored in the cfg
    noise_profiles = {nid: render_cfg.noises[nid] for nid in requested_noise_ids if
                      nid in render_cfg.noises}

    # 5. Resolve Physical Paths
    resolved_drivers = {dkey: render_cfg.path(dkey) for dkey in req_drivers}

    # 6. Determine the Geometry Anchor
    explicit_anchor = render_cfg.raw_defs.get("anchor")
    if explicit_anchor:
        anchor_key = to_enum(DriverKey, explicit_anchor)
    elif DriverKey.DEM in req_drivers:
        anchor_key = DriverKey.DEM
    else:
        anchor_key = sorted(list(req_drivers))[0] if req_drivers else None

    return RequiredResources(
        drivers=resolved_drivers, files=req_files, anchor_key=anchor_key,
        noise_profiles=noise_profiles, factor_inputs=preq.factor_names,
        surface_inputs=preq.surface_inputs, primary_surface=None, )


def analyze_pipeline(ctx: Any) -> tuple[bool, str]:
    """
    Performs a deep logical audit and generates a high-fidelity pipeline report.

    Validates:
    - Logic/Config parity (Ensures biome.yml covers all FACTOR_SPECS).
    - Smoothing logic presence (Ensures explicit rules for categorical data).
    - Modifier validity (Ensures intensity and noise sources are defined).
    - Sequence integrity (Ensures buffers/surfaces exist before use).
    """
    # from ThematicRender.settings import cfg.modifiers
    md = GenMarkdown()

    # 1. PREPARE CONTEXTUAL LOOKUPS
    cfg = ctx.render_cfg
    pipeline = ctx.pipeline
    fs_lookup = {fs.name: fs for fs in ctx.factors_engine.specs}
    ss_lookup = ctx.surfaces_engine.spec_registry

    warnings = []
    step_with_warnings = set()

    def add_warning(idx, msg):
        warnings.append(msg)
        if isinstance(idx, int):
            step_with_warnings.add(idx)

    # 1. PRE-SCAN: Identify everything the pipeline PROMISES to produce
    # This prevents transient surfaces from being flagged as "Missing from Library"
    pipeline_produced_keys = {step.output_surface for step in pipeline if
                              step.enabled and step.output_surface}
    library_surface_keys = {s.driver_id for s in cfg.surfaces}
    library_factor_names = {f.name for f in cfg.factors}

    # 2. SIMULATED STATE TRACKING
    # Start with surfaces actually defined in the YAML library
    sim_surfaces = set(library_surface_keys)
    sim_buffers = set()
    sim_factors = set(library_factor_names)

    # B. Validate Pipeline Sequence
    for i, step in enumerate(pipeline):
        if not step.enabled: continue

        # CHECK 1: Library Integrity
        if step.input_surfaces:
            for skey in step.input_surfaces:
                # If it's not in the library AND not something the pipeline produces, it's truly
                # missing
                if skey not in library_surface_keys and skey not in pipeline_produced_keys:
                    add_warning(
                        i, f"❌ **Missing Surfaces Item:**  _{skey.value}_ is required but not "
                           f"defined in YAML or produced by the pipeline."
                    )

        operator = COMPOSITING_REGISTRY.get(step.comp_op)
        if operator is None:
            add_warning(i, f"🔴 **Error:** Unknown operation `{step.comp_op}`.")
            continue

        # CHECK 2: Sequence Integrity (Does it exist YET?)
        if step.input_surfaces:
            for srf_key in step.input_surfaces:
                if srf_key not in sim_surfaces:
                    add_warning(
                        i, f"⚠️ **Sequence Warning:** Surface `{srf_key.value}` used before it was "
                           f"created."
                    )

        # CHECK 3: Factor Dependency
        if step.factor_nm and step.factor_nm not in sim_factors:
            add_warning(
                i, f"🔴 **Logic Error:** Factor `{step.factor_nm}` not defined in factors section."
            )

        # CHECK 4: Buffer Integrity
        if "buffer" in operator.required_attrs and step.comp_op != "create_buffer":
            if step.buffer not in sim_buffers:
                add_warning(i, f"🔴 **Buffer Error:** `{step.buffer}` has not been initialized.")

        # --- UPDATE SIMULATED STATE ---
        if step.comp_op == "create_buffer":
            sim_buffers.add(step.buffer)

        # If this step produces a surface, it is now available for subsequent steps
        if step.output_surface:
            sim_surfaces.add(step.output_surface)

    # --- 1. REPORT HEADER ---
    md.header("Thematic Render Pipeline Report", 1)
    md.bullet(f"{md.bold('Output:')} `{cfg.path('output')}`")
    md.bullet(f"{md.bold('Anchor:')} `{ctx.anchor_key.value}` (Geometry reference)")

    md.header("🚨  Warnings", 2)
    if warnings:
        for w in warnings: md.bullet(w)
    else:
        md.text("✅ No errors.")
    md.text("---")

    # --- 2. EXECUTION NARRATIVE ---
    md.header("1. Compositing Sequence", 2)

    for i, step in enumerate(pipeline):
        if not step.enabled: continue
        target = step.output_surface.value if step.output_surface else step.buffer
        warn_icon = "⚠️ " if i in step_with_warnings else ""

        md.header(f"Step {i}) [{target}] {warn_icon}{step.desc}", 3)
        md.bullet(f"{md.bold('Op:')} `{step.comp_op}`")

        # Factor Logic Breakdown
        if step.factor_nm:
            fs = fs_lookup.get(step.factor_nm)
            params = cfg.get_logic(step.factor_nm)
            na = float(params.get("noise_amp", 0.0))
            nap = float(params.get("noise_atten_power", 1.0))
            con = float(params.get("contrast", 1.0))
            sen = float(params.get("sensitivity", 1.0))
            mo = float(params.get("max_opacity", 1.0))
            md.bullet(f"{md.bold('Factor:')} `{step.factor_nm}`")
            if fs:
                md.text(
                    f"  * *Math:* `{fs.function_id}` using `"
                    f"{', '.join([d.value for d in fs.drivers])}`"
                )

            param_str = ", ".join([f"{k}: {v}" for k, v in params.items()])
            md.text(f"  * *Parameters:* `{param_str or 'None'}`")
            look_desc = describe_lerp_parms(na, nap, con, sen, mo)
            md.text(f"  * *Look:* **{look_desc}**")
            md.text(
                f"  * *Pipeline Shaping:* Scale={step.scale}, Bias={step.bias}, Contrast="
                f"{step.contrast}"
            )

        # Inbound Surface Details
        if step.input_surfaces:
            for srf_key in step.input_surfaces:
                ss = ss_lookup.get(srf_key)
                if ss:
                    md.bullet(f"{md.bold('Surface:')} `{srf_key.value}` ({ss.provider_id})")
                    if ss.modifiers:
                        mods = ", ".join([f"{m['id']}({m['profile_id']})" for m in ss.modifiers])
                        md.text(f"    * *Modifiers:* {mods}")
                else:
                    md.bullet(f"{md.bold('Buffer:')} `{srf_key.value}`")

    # --- 3. RESOURCE APPENDIX ---
    md.header("2. Global Resource Registry", 2)

    # Physical Drivers
    md.header("Input Drivers", 3)
    md.tbl_hdr("Driver Key", "Halo", "Cleanup")
    for dkey in sorted(list(ctx.eng_resources.drivers)):
        ds = cfg.get_spec(dkey)
        cleanup = f"{ds.cleanup_type} ({ds.smoothing_radius}px)" if ds.cleanup_type else "Raw"
        md.tbl_row(f"`{dkey.value}`", f"{ds.halo_px}px", cleanup)

    # Explicit Theme Smoothing
    md.header("Thematic Smoothing Rules", 3)
    md.tbl_hdr("Category", "Precedence", "Radius", "Grow Threshold")
    try:
        smooth_specs = cfg.get_smoothing_specs()
        for label, pspec in smooth_specs.items():
            md.tbl_row(
                label, pspec.get('precedence'), pspec.get('smoothing_radius'),
                pspec.get('expansion_weight')
            )
    except:
        md.text("*No smoothing rules defined.*")

    md.header("Surfaces", 2)
    md.tbl_hdr("Surface", "Base Provider", "Modifier ID", "Noise Source", "Shift (RGB)")
    for s_key in ctx.surface_inputs:
        ss = ss_lookup.get(s_key)
        if ss:
            mod = ss.modifiers[0] if ss.modifiers else None
            if mod:
                m_prof = cfg.modifiers.get(mod["profile_id"])
                md.tbl_row(
                    s_key.value, ss.provider_id, mod["profile_id"],
                    m_prof.noise_id if m_prof else "None",
                    str(m_prof.shift_vector) if m_prof else "N/A"
                )
            else:
                md.tbl_row(s_key.value, ss.provider_id, "None", "N/A", "N/A")

    # Surface Modifiers
    md.header("Surface Modifier Profiles (Mottling)", 3)
    md.tbl_hdr("ID", "Intensity", "RGB Shift Vector", "Noise Source")
    for mid, mprof in cfg.modifiers.items():
        md.tbl_row(f"`{mid}`", mprof.intensity, str(mprof.shift_vector), f"`{mprof.noise_id}`")

    # Noise Profiles
    md.header("Procedural Noise Profiles", 3)
    md.tbl_hdr("ID", "Sigmas", "Weights", "Stretch")
    for nid, nprof in ctx.eng_resources.noise_profiles.items():
        md.tbl_row(f"`{nid}`", str(nprof.sigmas), str(nprof.weights), str(nprof.stretch))

    # Themes ---
    md.header("Thematic Categories", 2)
    md.tbl_hdr("Label", "ID", "Opacity", "Noise Amp", "Edge Softness", "Status")

    label_to_id = ctx.theme_registry.name_to_id
    for label, cat_id in label_to_id.items():
        # Bridge QML Label to biome.yml Logic
        params = cfg.get_logic(label)

        # Determine Status
        if not params:
            status = "🟡 Using Defaults"
            amp = 0.3
            opac = 0.8
            blur = "N/A"
        else:
            status = "🟢 Configured"
            amp = params.get("noise_amp", 0.0)
            opac = params.get("max_opacity", 1.0)
            blur = f"{params.get('blur_px', 0)}px"

        # Highlight Transparency Leaks
        # If noise_amp > 0, the layer is mathematically NOT solid.
        opacity_desc = f"{opac * 100:.0f}%"
        if amp > 0:
            opacity_desc = f"**{opac * (1 - amp) * 100:.0f}% to {opac * 100:.0f}%**"
            status += " (Transparent Holes)"

        md.tbl_row(label, cat_id, opacity_desc, f"{amp * 100:.0f}%", blur, status)

    err_flag = len(warnings) > 0
    return err_flag, md.render()


def describe_lerp_parms(noise_amp, noise_atten_power, contrast, sensitivity, max_opacity) -> str:
    """
    Translates mathematical parameters into a qualitative description of the artistic look.
    """
    parts = []

    # 1. Texture/Variation (Noise Amp)
    if noise_amp < 0.1:
        parts.append("Smooth/Solid")
    elif noise_amp < 0.3:
        parts.append("Subtle Grain")
    elif noise_amp < 0.6:
        parts.append("Organic Mottling")
    else:
        parts.append("Aggressive Patchiness")

    # 2. Edge Character (Contrast)
    if contrast < 0.9:
        parts.append("Faded Edges")
    elif contrast <= 1.1:
        parts.append("Natural Transitions")
    elif contrast <= 2.5:
        parts.append("Crisp Boundaries")
    else:
        parts.append("Sharp/Clamped Edges")

    # 3. Shape Curve (Sensitivity / Power)
    if sensitivity < 0.8:
        parts.append("Broad Presence")
    elif sensitivity > 1.2:
        parts.append("Silky/Refined Falloff")

    # 4. Global Weight (Max Opacity)
    if max_opacity < 0.4:
        parts.append("Ghostly/Thin")
    elif max_opacity < 0.8:
        parts.append("Balanced Density")
    else:
        parts.append("Heavily Opaque")

    return ", ".join(parts)


def to_enum(key_cls, value):
    """Converts YAML strings to their corresponding Enum values with helpful hints."""
    if value is None:
        return None
    try:
        # Enums lookup by value
        return key_cls(value)
    except ValueError:
        # Extract all valid values from the Enum class
        valid_options = [item.value for item in key_cls]

        # Format the error message with the "Available" hint
        raise ValueError(
            f"❌ CONFIG ERROR: '{value}' is not a valid {key_cls.__name__}.\n"
            f"👉 Available: {', '.join(map(str, valid_options))}"
        ) from None  # 'from None' hides the original internal traceback for a cleaner CLI


def _parse_dtype(v: Any, *, where: str) -> np.dtype:
    """Parse dtype from config values."""
    if v is None:
        raise ValueError(f"{where}: dtype is None")

    if isinstance(v, np.dtype):
        return v

    if isinstance(v, type) and issubclass(v, np.generic):
        return np.dtype(v)

    if isinstance(v, str):
        key = v.strip()
        if key in DTYPE_ALIASES:
            return np.dtype(DTYPE_ALIASES[key])
        raise ValueError(f"{where}: unknown dtype string '{v}'")

    raise ValueError(f"{where}: unsupported dtype {type(v).__name__}: {v!r}")
