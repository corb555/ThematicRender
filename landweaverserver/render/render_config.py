from __future__ import annotations

# config_mgr.py
from dataclasses import dataclass
import hashlib
import json
from pathlib import Path
from typing import (Any, Tuple, Iterable, Set, Dict, List)

import numpy as np
from YMLEditor.yaml_reader import ConfigLoader

from landweaverserver.common.keys import SourceKey, FileKey, NoiseSpec, RequiredResources, _BlendSpec, SurfaceSpec, \
    FactorSpec, PipelineRequirements, SurfaceModifierSpec, SurfaceKey, SourceRndrSpec, DTYPE_ALIASES
from landweaverserver.render.schema import RENDER_SCHEMA
from landweaverserver.render.utils import  GenMarkdown


# render_config.py

class RenderConfigError(ValueError):
    """Raised when the render configuration is logically invalid or missing files."""
    pass


@dataclass(slots=True)
class RenderConfig:
    logic: Dict[str, Any]
    source_specs: Dict[SourceKey, SourceRndrSpec]
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
            raise ValueError(f"YAML Syntax Error in {config_path.name}: {e}")

        print(f"RenderConfig LOADING {config_path}")

        context = "initialization"
        current_item = "n/a"

        """
        Flatten the  dictionary into top level attributes
        """
        try:
            # 1. noises
            context = "noise_profiles"
            noises = {}
            for nid, data in defs.get("noise_profiles", {}).items():
                current_item = nid
                noises[nid] = NoiseSpec(
                    id=nid, sigmas=tuple(data["sigmas"]), weights=tuple(data["weights"]),
                    stretch=tuple(data.get("stretch", [1.0, 1.0])),
                    seed_offset=int(data.get("seed_offset", 0)), desc=data.get("desc", "")
                )

            # 2. modifiers
            context = "modifier_profiles"
            modifiers = {}
            for mid, data in defs.get("modifier_profiles", {}).items():
                current_item = mid
                modifiers[mid] = SurfaceModifierSpec(
                    intensity=float(data["intensity"]), shift_vector=tuple(data["shift_vector"]),
                    noise_id=data["noise_id"], desc=data.get("desc", "")
                )

            # 3. source_specs
            context = "source_specs"
            source_specs = {}
            for dname, data in defs.get("source_specs", {}).items():
                current_item = dname
                dkey = to_enum(SourceKey, dname)
                source_specs[dkey] = SourceRndrSpec(
                    dtype=data.get("dtype", "float32"), halo_px=int(data.get("halo_px", 64)),
                    #cleanup_type=data.get("cleanup_type"),
                    #smoothing_radius=float(data.get("smoothing_radius", 0)) if data.get(
                    #    "smoothing_radius"
                    #) else None
                )

            # 4. factors
            context = "factors"
            factors = []
            yaml_factors = {**defs.get("factors", {}), **defs.get("factor_specs", {})}
            for fname, data in yaml_factors.items():
                current_item = fname
                factors.append(
                    FactorSpec(
                        name=fname, factor_builder=data["factor_builder"],
                        sources=tuple(d for d in data["sources"]),
                        noise_id=data.get("noise_id"),
                        required_factors=tuple(data.get("required_factors", [])),
                        params=data.get("params", {}), desc=data.get("desc", "")
                    )
                )

            # 5. surfaces
            context = "surfaces"
            surfaces = []
            for sname, data in defs.get("surfaces", {}).items():
                current_item = sname
                input_f = data.get("input_factor")
                req_f = data.get("required_factors", [])
                if input_f and input_f not in req_f:
                    req_f.append(input_f)

                surfaces.append(
                    SurfaceSpec(
                        key=to_enum(SurfaceKey, sname), source=to_enum(SourceKey, data["source"]),
                        input_factor=data.get("input_factor"),
                        required_factors=tuple(req_f),
                        surface_builder=data.get("surface_builder"),
                        modifiers=data.get("modifiers", []),
                        files=tuple(data.get("files", [])),
                        desc=data.get("desc", "")
                    )
                )

            # 6. theme_render
            theme_render = defs.get("theme_render", {})

            # 6. theme_smoothing_specs
            theme_smoothing_specs = defs.get("theme_smoothing_specs", {})

            # 6. pipeline
            context = "pipeline"
            pipeline = []
            for idx, p_def in enumerate(defs.get("pipeline", [])):
                current_item = f"Step #{idx} ({p_def.get('blend_op', 'unknown')})"
                pipeline.append(
                    _BlendSpec(
                        desc=p_def.get("desc", ""), enabled=bool(p_def.get("enabled", True)),
                        blend_op=p_def["blend_op"], factor=p_def.get("factor"),
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
                logic=defs.get("logic", {}), source_specs=source_specs, files={}, raw_defs=defs,
                pipeline=pipeline, factors=factors, surfaces=surfaces, noises=noises,
                modifiers=modifiers, theme_render=theme_render,
                theme_smoothing_specs=theme_smoothing_specs
            )

        except KeyError as e:
            raise ValueError(
                f"Missing required field in [{context}] -> '{current_item}': {e}"
            )
        except Exception as e:
            raise ValueError(f"Logic error in [{context}] item '{current_item}': {e}")

    def resolve_paths(self, prefix: str, build_dir: Path, output_file: str) -> None:
        """Finalizes file dictionary and validates that all inputs exist on disk."""
        resolved_files = {}
        missing_files = []

        # 1. Standard Files (Direct paths like QML)
        # These are established first as the 'base' keys.
        for k, v in self.raw_defs.get("files", {}).items():
            p = Path(v).expanduser()
            if not p.exists():
                missing_files.append(f"Standard File [{k}]: {p}")
            resolved_files[k] = p

        # 2. Prefixed Files (Input TIFFs like _DEM.tif)
        for k, v in self.raw_defs.get("sources", {}).items():
            # The 'output' key is handled separately in step 3
            if k == "output":
                continue

            # --- THE COLLISION CHECK ---
            if k in resolved_files:
                raise ValueError(
                    f"❌ Configuration Error: Duplicate file key '{k}' detected. "
                    f"A key cannot be defined in both 'files' and 'sources'. "
                    f"Standard path: {resolved_files[k]} | Prefixed path: {v}"
                )

            p = (build_dir / f"{prefix}{v}").resolve()
            if not p.exists():
                missing_files.append(f"{k}: {p}")
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
    def get_halo_for_source(source):
        return 64

    def get_logic(self, key: str) -> Dict[str, Any]:
        """Returns math params (start, full, noise_amp)."""
        return self.logic.get(key, {})

    def get_spec(self, key: SourceKey, default: Any = None) -> SourceRndrSpec:
        """Returns hardware/storage specs (halo, cleanup_type)."""
        return self.source_specs.get(key, default)

    def get_smoothing_specs(self) -> Dict[str, Any]:
        """
        Returns the dictionary of thematic smoothing rules (precedence, radius, weight).
        """
        # 1. Try to pull from the 'theme_smoothing_specs' block in biome.yml
        return self.raw_defs.get("theme_smoothing_specs")

    def get_max_halo(self) -> int:
        """Return the maximum halo required by any configured source."""
        halos = [spec.halo_px for spec in self.source_specs.values() if
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

    def get_hashes(self) -> Dict[str, str]:
        """
        Declarative hash generation.
        Precedence: Top-level YAML sections override file-freshness checks.
        """
        hash_schema = {
            "topology": ["pipeline", "logic", "source_specs"],
            "logic": ["factors", "factor_specs", "noise_profiles"],
            "style": ["surfaces", "theme_render", "modifier_profiles", "theme_qml"]
        }

        hashes = {}

        for bucket, keys in hash_schema.items():
            bucket_data = {}

            for k in keys:
                # 1. PRECEDENCE: If the key is a top-level YAML section,
                # use the data directly and move to the next key.
                if k in self.raw_defs:
                    bucket_data[k] = self.raw_defs[k]
                    continue

                # TODO Remove hard-code of theme_qml and explicitly get from YAML config
                # 2. FALLBACK: If not in YAML, check if the key refers
                # to a physical file. If so, capture its modification time.
                if k == "theme_qml":
                    file_path = self.path(k)
                    bucket_data[f"{k}_mtime"] = file_path.stat().st_mtime

            # Generate the final deterministic hash for this bucket
            hashes[bucket] = self._generate_hash(bucket_data)

        return hashes

    @staticmethod
    def _generate_hash(data: dict) -> str:
        encoded = json.dumps(data, sort_keys=True).encode("utf-8")
        return hashlib.md5(encoded).hexdigest()


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

    # PASS 1: pipeline direct needs
    for step in active_steps:
        if step.factor: req_factors.add(step.factor)
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
                if spec.input_factor: req_factors.add(spec.input_factor)
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


def _require_blend_ops(pipeline_list: list[_BlendSpec], required_ops: set[str]) -> None:
    enabled = [s for s in pipeline_list if getattr(s, "enabled", True)]
    enabled_ops = {getattr(s, "blend_op", None) or getattr(s, "action", None) for s in enabled}
    enabled_ops.discard(None)

    missing = required_ops - enabled_ops
    if missing:
        pretty_enabled = [
            f"{i}: blend_op={getattr(s, 'blend_op', None)!r} target={getattr(s, 'target', None)!r}"
            for i, s in enumerate(enabled)]
        raise ValueError(
            "\n⚠️  PIPELINE CONFIG ERROR\n"
            f"Missing required pipeline steps: {sorted(missing)}\n"
            "Enabled steps:\n  - " + "\n  - ".join(pretty_enabled) + "\n"
                                                                     "Your pipeline must include "
                                                                     "an enabled "
                                                                     "blend_op='create_buffer' step "
                                                                     "before "
                                                                     "blend_op='write_output'.\n"
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

    req_sources: Set[SourceKey] = set()
    req_files: Set[FileKey] = {FileKey.RAMPS_YML}
    requested_noise_ids: Set[str] = set()

    # 2. Gather from Factors (Sources and Noises)
    for name in preq.factor_names:
        fs = fs_lookup.get(name)
        if fs:
            req_sources.update(fs.sources)
            if fs.noise_id:
                requested_noise_ids.add(fs.noise_id)

    # 3. Gather from Surfaces (Sources, Files, and Modifiers)
    for sk in preq.surface_inputs:
        ss = ss_lookup.get(sk)
        if ss:
            if ss.source: req_sources.add(ss.source)
            if ss.files: req_files.update(ss.files)

            if ss.modifiers:
                for mod_cfg in ss.modifiers:
                    profile_id = mod_cfg.get("mod_profile")
                    if profile_id:
                        v_profile = render_cfg.modifiers.get(profile_id)
                        if v_profile:
                            requested_noise_ids.add(v_profile.noise_id)
                        else:
                            raise ValueError(f"⚠️ Surface Effect Profile '{profile_id}' not found.")

    # 4. Fulfill Noise Profiles
    # Map the noise IDs to actual NoiseSpec objects stored in the cfg
    noise_profiles = {nid: render_cfg.noises[nid] for nid in requested_noise_ids if
                      nid in render_cfg.noises}

    # 5. Resolve Physical Paths
    resolved_sources = {dkey: render_cfg.path(dkey) for dkey in req_sources}

    # 6. Determine the Geometry Anchor
    explicit_anchor = render_cfg.raw_defs.get("anchor")
    anchor_key = to_enum(SourceKey, explicit_anchor)

    return RequiredResources(
        sources=resolved_sources, files=req_files, anchor_key=anchor_key,
        noise_profiles=noise_profiles, factor_inputs=preq.factor_names,
        surface_inputs=preq.surface_inputs, primary_surface=None, )


def analyze_pipeline(ctx: Any) -> tuple[bool, str, list]:
    """
    Performs a strict logical audit of the compositing sequence.
    """
    md = GenMarkdown()
    cfg = ctx.render_cfg
    pipeline = cfg.pipeline

    # 1. PREPARE CONTEXTUAL LOOKUPS
    # Map factor names to their specs (consolidated into factors)
    fs_lookup = {f.name: f for f in cfg.factors}
    ss_lookup = {s.key: s for s in cfg.surfaces}

    warnings = []
    step_with_warnings = set()

    def get_exact_val(item):
        if hasattr(item, 'value'): return item.value
        return str(item)

    def add_warning(idx, msg):
        warnings.append(msg)
        if isinstance(idx, int): step_with_warnings.add(idx)

    # 1. PRE-SCAN: Identities available in the Library
    library_surface_names = {get_exact_val(s.key) for s in cfg.surfaces}
    library_factor_names = {get_exact_val(f.name) for f in cfg.factors}

    # 2. SIMULATED STATE: Tracks what exists at each step
    sim_surfaces = set(library_surface_names)
    sim_factors = set(library_factor_names)
    sim_buffers = set()  # Buffers that have been created. canvas is default buffer
    sim_buffers.add("canvas")

    # 3. PIPELINE LOOP (The Narrative Audit)
    for i, step in enumerate(pipeline):
        if not step.enabled: continue

        # CHECK 1: Surface Inputs (Do they exist in library or previous steps?)
        if step.input_surfaces:
            for skey in step.input_surfaces:
                sname = get_exact_val(skey)
                if sname not in sim_surfaces and sname not in sim_buffers:
                    add_warning(
                        i, f"⚠️ **render Config error:** Surface/Buffer '{sname}' not found."
                        )

        # CHECK 2: Factor Dependency
        if step.factor:
            fname = get_exact_val(step.factor)
            if fname not in sim_factors:
                add_warning(i, f"⚠️ **render Config error:** Factor '{fname}' is not defined.")

        # CHECK 3: Buffer Sequence (Using a buffer before initialization)
        # Operators like 'lerp_buffers' or 'multiply' usually require an existing buffer
        if step.blend_op not in ["create_buffer"] and step.buffer:
            if step.buffer not in sim_buffers:
                add_warning(
                    i, f"⚠️ **render Config error:** Buffer '{step.buffer}' used before "
                       f"'create_buffer'. Available = '{sim_buffers}'"
                    )

        # --- UPDATE SIMULATED STATE ---
        if step.blend_op == "create_buffer":
            sim_buffers.add(step.buffer)

        if step.output_surface:
            sim_surfaces.add(get_exact_val(step.output_surface))

    # --- NOISE INTEGRITY CHECK ---
    # Centralized validation of all cross-references to noise_profiles
    noise_errors = validate_noise_integrity(cfg)
    warnings.extend(noise_errors)

    # --- REPORT GENERATION ---
    md.header("Land Weaver Pipeline Report", 1)
    md.bullet(f"{md.bold('Anchor:')} `{get_exact_val(ctx.anchor_key)}` (Spatial Reference)")

    md.header(" Warnings & Errors", 2)
    if warnings:
        for w in warnings: md.bullet(w)
    else:
        md.text("✅ pipeline configuration is logically sound.")
    md.text("---")

    # --- SECTION 1: EXECUTION NARRATIVE ---
    md.header("1. Compositing Sequence", 2)
    for i, step in enumerate(pipeline):
        if not step.enabled: continue
        target = get_exact_val(step.output_surface) if step.output_surface else step.buffer
        warn_icon = "⚠️ " if i in step_with_warnings else ""
        md.header(f"Step {i}) [{target}] {warn_icon}{step.desc}", 3)
        md.bullet(f"{md.bold('Op:')} `{step.blend_op}`")

        if step.factor:
            params = cfg.get_logic(get_exact_val(step.factor))
            md.bullet(f"{md.bold('Factor:')} `{step.factor}`")
            md.text(f"  * *Parameters:* `{params or 'Using defaults'}`")

    # --- SECTION 2: RESOURCE REGISTRY ---
    md.header("2. Global Resource Registry", 2)

    # Physical Sources (from source_specs)
    md.header("Input Sources", 3)
    md.tbl_hdr("Source Key", "Halo", "Cleanup")
    for dkey in sorted(list(ctx.eng_resources.pool_map.keys())):
        ds = cfg.get_spec(dkey)
        d_name = get_exact_val(dkey)
        #md.tbl_row(f"`{d_name}`", f"{ds.halo_px}px", "")

    # Noise Profiles
    md.header("Procedural Noise Profiles", 3)
    md.tbl_hdr("ID", "Sigmas", "Weights")
    for nid, nprof in cfg.noises.items():
        md.tbl_row(f"`{nid}`", str(nprof.sigmas), str(nprof.weights))

    # Themes (Categorical Logic)
    md.header("Land Weaver Categories", 2)
    md.tbl_hdr("Label", "ID", "Opacity", "Noise Amp", "Status")

    # categories live in theme_render
    """    theme_cats = cfg.theme_render.get("categories", {})
    for label, cat_id in ctx.theme_registry.name_to_id.items():
        params = theme_cats.get(label)
        if not params:
            md.tbl_row(label, cat_id, "N/A", "N/A", "🟡 Missing Settings")
        else:
            opac = params.get("max_opacity", 1.0)
            amp = params.get("noise_amp", 0.0)
            md.tbl_row(label, cat_id, f"{opac*100:.0f}%", f"{amp*100:.0f}%", "🟢 Active")
"""
    clean_errors = [w.replace("**", "").replace("_", "").replace("`", "") for w in warnings]
    return len(warnings) > 0, md.render(), clean_errors


def validate_noise_integrity(render_cfg: Any) -> list[str]:
    """Checks all cross-references to the noises library. Returns list of error strings."""
    valid_noises = set(render_cfg.noises.keys()) if hasattr(render_cfg, 'noises') else set()
    errors = []

    # 1. Check Theme Categories -> Noise
    theme_render = getattr(render_cfg, "theme_render", {}) or {}
    categories = theme_render.get("categories", {})
    for label, cat in categories.items():
        n_id = cat.get("noise_id")
        # Note: we ignore "none" or empty strings as they are valid 'no-noise' states
        if n_id and n_id != "none" and n_id not in valid_noises:
            errors.append(
                f"❌ **Theme Logic Error:** `{label}` references unknown noise id `{n_id}`"
            )

    # 2. Check Surface Modifiers -> Noise
    modifiers = getattr(render_cfg, "modifiers", {}) or {}
    for mid, mprof in modifiers.items():
        if mprof.noise_id and mprof.noise_id not in valid_noises:
            errors.append(
                f"❌ **Modifier Error:** Profile `{mid}` references unknown noise id `{mprof.noise_id}`"
            )

    # 3. Check Factor Engine -> Noise
    factors = getattr(render_cfg, "factors", {}) or {}

    for f in factors:
        # Access attributes directly from the object
        # (Assuming the object has .noise_id and .name)
        n_id = getattr(f, "noise_id", None)

        if n_id and n_id != "none" and n_id not in valid_noises:
            f_name = getattr(f, "name", "Unknown Factor")
            errors.append(
                f"❌ **Factor Logic Error:** Factor `{f_name}` references unknown noise id `{n_id}`"
            )
    return errors


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
    return value


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
