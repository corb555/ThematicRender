import argparse
from pathlib import Path
import sys

from ThematicRender.config_mgr import ConfigMgr, analyze_pipeline
from ThematicRender.pipeline_engine import PipelineEngine
from ThematicRender.settings import BLEND_PIPELINE, SURFACE_SPECS, FACTOR_SPECS

import sys
import argparse
from pathlib import Path

from ThematicRender.config_mgr import ConfigMgr, analyze_pipeline
from ThematicRender.pipeline_engine import PipelineEngine
from ThematicRender.settings import BLEND_PIPELINE, FACTOR_SPECS, SURFACE_SPECS

def _validate_file_existence(cfg: ConfigMgr):
    """
    Since validate_paths was removed from ConfigMgr to keep it logic-only,
     we perform the disk check here before starting the engine.
    """
    for key, path in cfg.files.items():
        if key == "output":
            if not path.parent.exists():
                raise FileNotFoundError(f"Output directory missing: {path.parent}")
            continue

        if not path.exists():
            raise FileNotFoundError(f"Required input file missing: [{key}] -> {path}")

def main():
    parser = argparse.ArgumentParser(description="Thematic Render Pipeline")

    # Positional Arguments
    parser.add_argument("prefix", help="Path prefix (e.g. 'build/Profile/Profile')")
    parser.add_argument("output", help="Output path override")

    # Required Arguments
    parser.add_argument("--config", required=True, help="Path to the YAML config file")

    # Operational flags
    parser.add_argument("--describe", action="store_true", help="Generate pipeline description")
    parser.add_argument("--describe_only", action="store_true", help="Generate description and EXIT")
    parser.add_argument("--multi", action="store_true", help="Multiprocess")

    # Preview Params
    parser.add_argument("--percent", type=float, help="Build preview version (0.0 to 1.0)")
    parser.add_argument("--row", type=float, help="Focal point Y (0.0 to 1.0)")
    parser.add_argument("--col", type=float, help="Focal point X (0.0 to 1.0)")

    print("Thematic Render")
    args = parser.parse_args()

    # 1. Load, Fuse, and Resolve Config
    try:
        # The new atomic factory method
        config = ConfigMgr.build(
            config_path=Path(args.config),
            prefix=args.prefix,
            output_override=args.output
        )

        # Perform physical disk checks
        _validate_file_existence(config)

    except (ValueError, FileNotFoundError, KeyError) as e:
        print(f"\n❌ Configuration Error: {e}")
        return

    print(f"Config File: {args.config}\n")

    # 2. Initialize Engine
    pipeline_eng = PipelineEngine(config, BLEND_PIPELINE, args.percent, args.row, args.col, args.multi)

    # 3. Handle Documentation / Diagnostics
    if args.describe or args.describe_only:
        desc_file = f"{args.prefix}_describe.md"

        # analyze_pipeline now uses the simplified ConfigMgr
        report = analyze_pipeline(
            cfg=pipeline_eng.cfg,
            resources=pipeline_eng.resources,
            pipeline=pipeline_eng.pipeline,
            factor_specs=FACTOR_SPECS,
            surface_specs=SURFACE_SPECS
        )

        with open(desc_file, "w") as f:
            f.write(report)
        print(f"✅ Pipeline description generated in: {desc_file}\n")

        if args.describe_only:
            sys.exit(0)

    # 4. Execute Render
    try:
        if args.multi:
            print("Processing rasters. [Multi processor]")
        else:
            print("Processing rasters. [Single processor]")

        pipeline_eng.process_rasters()
        print("✅ Success.")
    except Exception as e:
        print(f"\n❌ Pipeline error: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    main()
