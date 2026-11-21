import gdb, os, time

# Diagnostic only. Samples every thread's stack while the first blocks arrive.
# Frame names are cut short: Rust generic names run to kilobytes.
gdb.execute("set pagination off")
gdb.execute("handle SIGINT stop nopass")
pid = gdb.selected_inferior().pid
SAMPLES = int(os.environ.get("DIAG_SAMPLES", "70"))
start = time.time()


def stack(thread, limit=28):
    thread.switch()
    out = []
    frame = gdb.newest_frame()
    while frame is not None and len(out) < limit:
        name = frame.name() or "??"
        cut = name.find("<")
        if cut > 0:
            name = name[:cut]
        sal = frame.find_sal()
        where = ""
        if sal and sal.symtab:
            where = " @%s:%d" % ("/".join(sal.symtab.filename.split("/")[-2:]), sal.line)
        out.append(name[:100] + where)
        try:
            frame = frame.older()
        except gdb.error:
            break
    return out


for i in range(SAMPLES):
    print("=== sample %d t=%.2f" % (i, time.time() - start))
    for t in gdb.selected_inferior().threads():
        try:
            frames = stack(t)
        except gdb.error as e:
            frames = ["<%s>" % e]
        # Idle pool threads say nothing.
        joined = " ".join(frames[:6])
        if "epoll_wait" in joined and len(frames) < 12:
            continue
        print("--- thread %s %s" % (t.num, t.name))
        for f in frames:
            print("   " + f)
    os.system("(sleep 0.2; kill -INT %d) &" % pid)
    gdb.execute("continue")
gdb.execute("detach")
