debug_mode = False
limit = 10
if debug_mode:
    #print(f"🔬 DEBUG MODE: Processing first {limit} blocks...")
    active_windows = win_list[:limit]
    iterable = enumerate(active_windows)
else:
    active_windows = win_list
    iterable = enumerate(tqdm(active_windows, desc="Rendering (ST)"))

max_halo = self.render_cfg.get_max_halo()
for tile_seq, window in iterable:
    work_packet = build_work_packet_for_window(
        tile_seq=tile_seq, window=window, ctx=reader_ctx, registry=self.registry,
        resources=self.eng_resources, io_for_geom=io, max_halo=max_halo, )
    result_packet = render_task(packet=work_packet, ctx=worker_ctx)
    stats["render"] += result_packet.render_duration
    stats["write"] += write_task(packet=result_packet, ctx=writer_ctx)
    stats["count"] += 1

f_start = time.perf_counter()
writer_ctx.close()
stats["write"] += (time.perf_counter() - f_start)
