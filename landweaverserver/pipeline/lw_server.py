import argparse

from landweaverserver.pipeline.pipeline_engine import PipelineEngine
from landweaverserver.render.render_config import RenderConfig


# from LandWeaverServer.settings import BLEND_PIPELINE


def _validate_file_existence(cfg: RenderConfig):
    """
    Since validate_paths was removed from ConfigMgr to keep it logic-only,
     we perform the disk check here before starting the engine.
    """
    if cfg is None:
        raise ValueError("_validate_file_existence: cfg is None")

    for key, path in cfg.files.items():
        if key == "output":
            if not path.parent.exists():
                raise FileNotFoundError(f"Output directory missing: {path.parent}")
            continue

        if not path.exists():
            raise FileNotFoundError(f"Required input file missing: [{key}] -> {path}")


def main():
    parser = argparse.ArgumentParser(description="Land Weaver Server")

    # Positional Arguments
    parser.add_argument("build_dir", help="Path prefix (e.g. 'build/Sedona')")
    parser.add_argument("region_prefix", help="Path prefix (e.g. 'Sedona')")

    parser.add_argument("output", help="Output path override")

    # Required Arguments
    parser.add_argument("--config", required=True, help="Path to the YAML config file")

    # Operational flags
    parser.add_argument("--describe", action="store_true", help="Generate pipeline description")
    parser.add_argument(
        "--describe_only", action="store_true", help="Generate description and EXIT"
    )
    parser.add_argument("--multi", action="store_true", help="Multiprocess")

    # Preview Params
    parser.add_argument("--percent", type=float, help="Build preview version (0.0 to 1.0)")
    parser.add_argument("--row", type=float, help="Focal point Y (0.0 to 1.0)")
    parser.add_argument("--col", type=float, help="Focal point X (0.0 to 1.0)")

    print("Land Weaver Server")
    #  BETA WARNING
    print("!  " + "NOTICE: THIS IS BETA SOFTWARE. DO NOT USE FOR PRODUCTION.".center(64) + "  !")
    print("!  " + "Features and configuration will change without notice.".center(64) + "  !")
    args = parser.parse_args()

    # 1. Load and Resolve Config
    """    try:
        config = RenderConfig.load(
            config_path=Path(args.config), build_dir=args.build_dir,
            region_prefix=args.region_prefix, output_override=args.output
        )

        # Perform physical disk checks
        _validate_file_existence(config)

    # except (ValueError, FileNotFoundError, KeyError) as e:
    except MemoryError as e:
        print(f"\n❌ Configuration Error: {e}")
        sys.exit(-1)"""

    print(f"System Config File: {args.config}")

    # 2. Initialize Engine
    pipeline_eng = PipelineEngine(args.config)

    # 4. Execute render
    try:
        pipeline_eng.start()
        print("❌ Engine Closed due to error. Shutdown completed.")
    except MemoryError as e:
        print(f"\n❌ pipeline error: {e}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    main()
