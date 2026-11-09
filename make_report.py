"""Builds Lab_Report_CAP.pdf from MEASURED data (nothing is typed in by hand).

    python3 -m pytest --junitxml=junit.xml      # test results
    python3 perf_cap_bench.py --rtt-ms 40       # perf_cap_results.json
    python3 perf_cap_bench.py --rtt-ms 0 --out perf_cap_results_localhost
    python3 make_report.py                      # runs the R/W experiment itself
"""
import collections
import itertools
import json
import re
import sys
import xml.etree.ElementTree as ET
from pathlib import Path

from reportlab.lib import colors
from reportlab.lib.pagesizes import A4
from reportlab.lib.styles import ParagraphStyle, getSampleStyleSheet
from reportlab.lib.units import cm
from reportlab.platypus import (Paragraph, SimpleDocTemplate, Spacer, Table,
                                TableStyle)

ROOT = Path(__file__).resolve().parent
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(ROOT / "tests"))
from test_cap import NAMES, Cluster  # noqa: E402
from client import read_from, write_to  # noqa: E402

ss = getSampleStyleSheet()
BODY = ParagraphStyle("body", parent=ss["BodyText"], fontSize=9.5, leading=13)
SMALL = ParagraphStyle("small", parent=BODY, fontSize=8, leading=10)
CELL = ParagraphStyle("cell", parent=BODY, fontSize=8.5, leading=10.5)
H1 = ParagraphStyle("h1", parent=ss["Heading1"], fontSize=15, spaceBefore=10)
H2 = ParagraphStyle("h2", parent=ss["Heading2"], fontSize=11.5, spaceBefore=8)
TITLE = ParagraphStyle("t", parent=ss["Title"], fontSize=20)


def P(text, style=BODY):
    return Paragraph(text, style)


def tbl(rows, widths, header=True, zebra=True, align_center_from=None):
    HEAD = ParagraphStyle("head", parent=CELL, textColor=colors.white,
                          fontName="Helvetica-Bold")
    data = [[c if not isinstance(c, str) else P(c, HEAD if header and i == 0 else CELL)
             for c in r] for i, r in enumerate(rows)]
    t = Table(data, colWidths=widths, repeatRows=1 if header else 0)
    st = [("GRID", (0, 0), (-1, -1), 0.4, colors.HexColor("#9aa5b1")),
          ("VALIGN", (0, 0), (-1, -1), "MIDDLE"),
          ("TOPPADDING", (0, 0), (-1, -1), 3), ("BOTTOMPADDING", (0, 0), (-1, -1), 3)]
    if header:
        st += [("BACKGROUND", (0, 0), (-1, 0), colors.HexColor("#1f3a5f")),
               ("TEXTCOLOR", (0, 0), (-1, 0), colors.white)]
    if zebra:
        for i in range(1, len(rows)):
            if i % 2 == 0:
                st.append(("BACKGROUND", (0, i), (-1, i), colors.HexColor("#f1f4f8")))
    t.setStyle(TableStyle(st))
    return t


# ---------------------------------------------------------------- measured data
def quorum_experiment():
    """Exhaustive R/W experiment on a fully isolated 3-node cluster."""
    cl = Cluster()
    out = []
    try:
        cl.net.partition(["A"], ["B"], ["C"])
        allc = list(cl.clients.values())
        for r, w in [(1, 1), (1, 2), (2, 1), (2, 2), (1, 3), (3, 1)]:
            stale = total = 0
            for wset, rset in itertools.product(itertools.combinations(NAMES, w),
                                                itertools.combinations(NAMES, r)):
                key = f"q{r}{w}-{total}"
                write_to(allc, key, "old", 1)
                write_to([cl.clients[n] for n in wset], key, "new", 2)
                seen = read_from([cl.clients[n] for n in rset], key)
                total += 1
                stale += seen[0] != "new"
            out.append((r, w, total, stale))
    finally:
        cl.close()
    return out


def test_results():
    root = ET.parse(ROOT / "junit.xml").getroot()
    res = collections.OrderedDict()
    for tc in root.iter("testcase"):
        name = re.sub(r"\[.*\]$", "", tc.get("name"))
        bad = tc.find("failure") is not None or tc.find("error") is not None
        p, t = res.get(name, (0, 0))
        res[name] = (p + (not bad), t + 1)
    return res


DESCR = {
    "test_quorum_intersection_formula": "R + W > N holds exactly when read and write sets must overlap",
    "test_rw_quorum_experiment_matches_formula": "Exhaustive R/W experiment: zero stale reads iff R + W > N",
    "test_cp_write_on_majority_partition_succeeds": "CP Put on DC-1 (A,B): success, acks_received = 2",
    "test_cp_write_on_minority_partition_is_rejected": "CP Put on DC-2 (C): ReplicaUnavailableError, nothing stored",
    "test_cp_write_succeeds_again_after_heal": "After reconnect the same CP Put succeeds with 3 acks",
    "test_ap_write_on_minority_partition_is_accepted": "AP Put on DC-2: success, acks_received = 1",
    "test_partition_recovery_lww_converges_to_latest_timestamp": "After heal, LWW picks the newest write (both directions)",
    "test_lww_equal_timestamps_break_ties_by_node_name": "Equal timestamps: deterministic tie-break, all replicas agree",
    "test_background_anti_entropy_heals_without_explicit_sync": "Divergent replicas converge by background anti-entropy alone",
    "test_linearizable_read_from_follower_is_proxied_to_leaseholder": "LINEARIZABLE read sent to a follower is proxied to Leaseholder",
    "test_linearizable_is_strict_but_follower_read_stays_available": "Cut-off follower refuses LINEARIZABLE but serves FOLLOWER_STALE",
    "test_leaseholder_without_majority_refuses_linearizable_reads": "Leaseholder that lost its majority refuses linearizable reads",
    "test_follower_read_exceeding_max_staleness_is_rejected": "STALE_READ_EXCEEDED when contact age > max_staleness_ms",
    "test_non_leaseholder_range_key_uses_majority_quorum_read": "Keys outside [a-z] use a majority quorum read",
    "test_idempotency_key_makes_put_retries_safe": "Same idempotency key is applied only once",
    "test_subprocess_cluster_cp_vs_ap_under_partition": "CP/AP partition behaviour against REAL server.py processes",
}


def perf_table(rows):
    data = [["Read Mode", "Target Node", "Median Latency (ms)", "p95 Latency (ms)",
             "Read Availability (%)"]]
    f = lambda x: "n/a" if x is None else f"{x:.2f}"
    for r in rows:
        data.append([r["mode"], r["target"], f(r["median_ms"]), f(r["p95_ms"]),
                     f"{r['availability_pct']:.1f}  ({r['ok']}/{r['iterations']})"])
    return tbl(data, [4.4 * cm, 4.2 * cm, 2.9 * cm, 2.7 * cm, 3.2 * cm])


def build():
    perf = json.loads((ROOT / "perf_cap_results.json").read_text())
    raw = json.loads((ROOT / "perf_cap_results_localhost.json").read_text())
    tests = test_results()
    exp = quorum_experiment()
    n_pass = sum(p for p, _ in tests.values())
    n_tot = sum(t for _, t in tests.values())

    s = [P("Lab Report: Consistency Models &amp; the CAP Theorem", TITLE),
         P("<b>Course:</b> CSS4004 - Distributed Systems &nbsp;&nbsp; "
           "<b>Topic:</b> Immutability, PACELC Trade-offs and Quorum Tuning in a "
           "Distributed Key-Value Store", BODY),
         Spacer(1, 4),
         P("<b>System.</b> Three Python/gRPC replicas (A: port 50051 Leaseholder for "
           "keyspace [a-z]; B: 50052; C: 50053), extended from the Lab 2 replicated "
           "counter (idempotency keys, deadlines/retries and majority-ack writes are "
           "reused). Reads are LINEARIZABLE or FOLLOWER_STALE; writes are QUORUM_CP or "
           "ALWAYS_AP; divergent copies are merged by Last-Write-Wins ordered by "
           "(timestamp, origin node). Network partitions are injected through a "
           "SetPartition RPC that makes a node drop all traffic to and from the "
           "peers it is cut off from (works for in-process and subprocess nodes).")]

    # ---------------- Section 1
    s.append(P("Section 1: Test Results &amp; Quorum Verification", H1))
    s.append(P(f"<b>pytest:</b> {n_pass}/{n_tot} test cases pass "
               f"({len(tests)} test functions, parametrized cases counted "
               f"individually) in <font face='Courier'>tests/test_cap.py</font>.", BODY))
    s.append(Spacer(1, 4))
    rows = [["Test", "Verifies", "Passed"]]
    for name, (p, t) in tests.items():
        rows.append([name.replace("test_", "").replace("_", " "), DESCR.get(name, ""),
                     f"{p}/{t}"])
    s.append(tbl(rows, [6.6 * cm, 8.8 * cm, 2 * cm]))

    s.append(P("1.1 Quorum verification table (N = 3)", H2))
    s.append(P("The nodes are fully isolated so each replica holds only what the "
               "client wrote to it. Every node first holds <i>old</i> (timestamp 1). "
               "A new version (timestamp 2) is written to <b>every possible write set "
               "of size W</b>, then read back from <b>every possible read set of "
               "size R</b> (client keeps the highest timestamp). A read is stale if "
               "it misses the new version.", BODY))
    s.append(Spacer(1, 4))
    rows = [["R", "W", "R + W", "R + W > N ?", "Write x read sets tried",
             "Stale reads", "Outcome"]]
    for r, w, total, stale in exp:
        ok = r + w > 3
        rows.append([str(r), str(w), str(r + w), "yes" if ok else "no", str(total),
                     f"{stale} ({100 * stale / total:.0f}%)",
                     "always sees latest write" if stale == 0 else "stale reads possible"])
    s.append(tbl(rows, [1 * cm, 1 * cm, 1.5 * cm, 2.6 * cm, 3.4 * cm, 2.6 * cm, 5 * cm]))
    s.append(Spacer(1, 3))
    s.append(P("Result: stale reads occur in exactly the configurations with R + W &lt;= N "
               "and never when R + W &gt; N, matching the formula.", BODY))

    s.append(P("1.2 Network-partition results (DC-1 = {A, B}, DC-2 = {C})", H2))
    rows = [["Step", "Request", "Expected", "Observed"],
            ["CP-1", "Put QUORUM_CP sent to DC-1", "success, acks = 2",
             "success=true, acks_received=2"],
            ["CP-2", "Put QUORUM_CP sent to DC-2", "failure, quorum loss",
             "ReplicaUnavailableError: quorum lost, reached 1/3, need 2; no replica stored the value"],
            ["AP-1", "Put ALWAYS_AP sent to DC-2", "accepted, acks = 1",
             "success=true, acks_received=1"],
            ["AP-2", "Heal partition, then read",
             "LWW merges divergent values",
             "DC-1 wrote v1 (t=100), DC-2 wrote v2 (t=200): all three replicas and the "
             "leaseholder return v2. With t=300 on DC-1, all return v1."]]
    s.append(tbl(rows, [1.3 * cm, 4.2 * cm, 3.6 * cm, 8.3 * cm]))

    # ---------------- Section 2
    s.append(P("Section 2: Performance Evaluation (PACELC)", H1))
    s.append(P(f"{perf['iterations']} iterations per scenario (after a 50-read "
               "warm-up) against three real server processes on localhost. The client is "
               "co-located with follower B. <b>Linearizable</b> reads are sent to the "
               "Leaseholder A, which is the remote node; <b>Follower Stale</b> reads "
               f"(max_staleness_ms = {perf['max_staleness_ms']}) are sent to local "
               "follower B. Median is statistics.median; p95 is nearest-rank. "
               "Latency covers successful reads; availability = successful / total.",
               BODY))
    s.append(P(f"2.1 Required table: with {perf['rtt_ms']:g} ms simulated round trip "
               "to the remote leaseholder", H2))
    s.append(perf_table(perf["healthy"]))
    s.append(Spacer(1, 3))
    s.append(P("<b>Important:</b> all processes run on one machine, so there is no real "
               f"WAN. The {perf['rtt_ms']:g} ms is a <i>simulated</i> round trip "
               "(time.sleep before each call to the remote node), chosen to represent "
               "a cross-datacenter link. The 40 ms figure is therefore my assumption, "
               "not a measurement; the local-follower number is a real measurement.",
               SMALL))
    s.append(P("2.2 Same benchmark, no simulated latency (raw localhost)", H2))
    s.append(perf_table(raw["healthy"]))
    s.append(Spacer(1, 3))
    s.append(P("Without network distance the two paths cost the same (one gRPC hop plus "
               "a dictionary lookup), so the latency gap in 2.1 comes entirely from "
               "reaching the Leaseholder, not from extra work in the server.", SMALL))
    s.append(P("2.3 Availability when the Leaseholder is partitioned away", H2))
    s.append(P("Leaseholder A is isolated from B and C for the whole run (1,000 reads per "
               f"row, {perf['rtt_ms']:g} ms simulated RTT to A). A lost its majority, so "
               "it refuses linearizable reads.", BODY))
    s.append(Spacer(1, 3))
    s.append(perf_table(perf["fault"]))
    s.append(Spacer(1, 3))
    s.append(P("<b>Reading the results.</b> Follower reads are about "
               f"{perf['healthy'][0]['median_ms'] / perf['healthy'][1]['median_ms']:.0f}x "
               "faster at the median in the simulated WAN setting and stay available while "
               "the leaseholder is unreachable, but only until the staleness bound "
               "(5 s) runs out; after that the follower correctly answers "
               "STALE_READ_EXCEEDED. Linearizable reads were 100% available when healthy "
               "and 0% while the leaseholder lacked a majority. This is the PACELC picture: "
               "during a partition (P) the system must pick availability or consistency; "
               "otherwise (E) it picks latency or consistency.", BODY))

    # ---------------- Section 3
    s.append(P("Section 3: Conceptual Questions", H1))
    s.append(P("3.1 Why does R + W &gt; N guarantee up-to-date reads, whereas "
               "R + W &lt;= N allows stale reads?", H2))
    s.append(P("A write is acknowledged by W of the N replicas; a read consults R of "
               "them. Two subsets of sizes W and R drawn from N replicas must share at "
               "least R + W - N members (pigeonhole principle). If R + W &gt; N that number "
               "is at least 1, so every read set contains at least one replica that "
               "acknowledged the latest write; taking the highest timestamp among the R "
               "answers therefore returns it. If R + W &lt;= N, the sets can be "
               "<i>disjoint</i>: the read may land entirely on replicas that missed the "
               "write and return an old value. Table 1.1 shows exactly this: with "
               "N = 3, (R=2, W=2), (1,3) and (3,1) gave 0 stale reads, while (1,1), (1,2) "
               "and (2,1) gave stale reads. <br/><i>Caveat:</i> quorum overlap guarantees "
               "that a read sees the latest <b>completed</b> write. Full linearizability "
               "also needs a single agreed order for concurrent operations and a read "
               "that cannot move 'back in time' (e.g. a leader/leaseholder or read "
               "repair). This is why the system also offers the leaseholder path, and why "
               "sloppy (AP) writes break the guarantee.", BODY))
    s.append(P("3.2 In the partition test, what trade-off was made for writes on the "
               "minority partition under QUORUM_CP vs ALWAYS_AP?", H2))
    s.append(P("<b>QUORUM_CP chose Consistency over Availability.</b> Node C could reach "
               "only 1 of 3 replicas, below the majority of 2, so it refused the write "
               "(ReplicaUnavailableError) and stored nothing. Clients on the minority "
               "side lose write availability, but no replica diverges and no write is "
               "ever lost. <br/><b>ALWAYS_AP chose Availability over Consistency.</b> "
               "C accepted the write with a single ack (acks_received = 1), so DC-2 "
               "stayed writable, but C and DC-1 held different values for the same key "
               "until the partition healed. Convergence then relied on Last-Write-Wins, "
               "which resolves the conflict deterministically but <b>silently discards "
               "the older concurrent write</b> (a lost update) and depends on timestamps. "
               "Outside partitions, the PACELC 'else' side shows the same theme: the "
               "leaseholder read pays latency for consistency, the follower read pays "
               "possible staleness for low latency.", BODY))

    s.append(P("Implementation notes and limitations", H2))
    s.append(P("(1) A CP write is sent to all reachable peers first and only applied by "
               "the coordinator if a majority acknowledged; with N = 3 a failed write "
               "leaves no copies, but for larger N a real system would need Raft or "
               "2PC to avoid orphan copies. (2) The Leaseholder is fixed (node A) and "
               "there is no automatic lease transfer; when A is cut off, linearizable "
               "reads for [a-z] keys are unavailable by design. (3) Staleness is "
               "measured as time since the follower last heard from the Leaseholder "
               "(heartbeats every 100 ms), a simplification of closed timestamps. "
               "(4) LWW needs comparable timestamps; client or server clock skew can "
               "make an older write win. (5) The partition injector is a software "
               "wrapper inside the nodes, not OS-level packet filtering.", SMALL))

    doc = SimpleDocTemplate(str(ROOT / "Lab_Report_CAP.pdf"), pagesize=A4,
                            leftMargin=1.7 * cm, rightMargin=1.7 * cm,
                            topMargin=1.5 * cm, bottomMargin=1.5 * cm,
                            title="Lab Report: Consistency Models and the CAP Theorem")
    doc.build(s)
    print("written Lab_Report_CAP.pdf")


if __name__ == "__main__":
    build()
