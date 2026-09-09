// SPDX-License-Identifier: Apache-2.0
(() => {
  "use strict";
  const { coverage, samples } = JSON.parse(document.getElementById("dataset").textContent);
  const byId = (id) => document.getElementById(id);
  const svg = (name, attrs = {}) => {
    const node = document.createElementNS("http://www.w3.org/2000/svg", name);
    for (const [key, value] of Object.entries(attrs)) node.setAttribute(key, String(value));
    return node;
  };
  const label = (parent, value) => {
    const node = document.createElement("span");
    node.textContent = value;
    parent.append(node);
    return node;
  };
  const card = (title, value) => {
    const container = document.createElement("div");
    container.className = "card";
    label(container, title);
    const count = document.createElement("strong");
    count.textContent = value;
    container.append(count);
    byId("coverage").append(container);
  };
  card("DSLX files", coverage.input_files);
  card("Files examined", coverage.scanned_files);
  card("Functions discovered", coverage.discovered_functions);
  card("Concrete attempts", coverage.attempted_functions);
  card("Optimized functions", coverage.optimized_functions);
  card("Parametric skipped", coverage.parametric_skipped);
  card("Oversize k-cones", coverage.k_oversize_skipped);
  card("Unique cones", coverage.unique_cones);
  card("Cone occurrences", coverage.cone_occurrences);
  card("Compared cones", coverage.completed_pairs);
  card("Comparison samples", coverage.comparison_samples);
  card("Ingest failures", coverage.ingest_failures.length);
  card("Comparison failures", coverage.comparison_failures.length);

  function outcome(sample) {
    const nodes = Math.sign(sample.g8r_nodes - sample.yosys_nodes);
    const depth = Math.sign(sample.g8r_depth - sample.yosys_depth);
    return nodes < 0 && depth < 0 ? "win" : nodes > 0 && depth > 0 ? "loss" : "mixed";
  }
  const kinds = [...new Set(samples.map((sample) => sample.kind))].sort();
  for (const kind of kinds) {
    const option = document.createElement("option");
    option.value = kind;
    option.textContent = kind;
    byId("kind").append(option);
  }

  const detail = byId("detail");
  function show(sample) {
    detail.replaceChildren();
    const heading = document.createElement("h3");
    heading.textContent = `${sample.kind}: ${sample.origins[0]?.function || sample.sample_id}`;
    detail.append(heading);
    const link = document.createElement("a");
    link.href = sample.cone_ir;
    link.textContent = "Open exact cone IR";
    detail.append(link);
    const text = document.createElement("pre");
    text.textContent = [
      `Sample: ${sample.sample_id}`,
      `Cone top: ${sample.ir_top}; IR ops: ${sample.ir_op_count ?? "unknown"}`,
      `G8r+ABC: ${sample.g8r_nodes} AND nodes, ${sample.g8r_depth} levels`,
      `Codegen+Yosys/ABC: ${sample.yosys_nodes} AND nodes, ${sample.yosys_depth} levels`,
      "",
      "Source occurrences (overlaps are preserved):",
      ...sample.origins.map((o) => `  ${o.file} :: ${o.function} (optimized IR ${o.source_ir_sha256}; MFFC rank ${o.rank ?? "n/a"})`),
      "",
      `DSLX driver: ${coverage.driver_version}`,
      `Comparison driver: ${sample.driver_crate_version}; DSO: ${sample.dso_version}`,
      `ABC script SHA-256: ${sample.yosys_script_sha256}`,
      `G8r ABC action: ${sample.g8r_abc_aig_action_id}`,
      `G8r stats action: ${sample.g8r_stats_action_id}`,
      `Yosys ABC action: ${sample.yosys_aig_action_id}`,
      `Yosys stats action: ${sample.yosys_stats_action_id}`,
    ].join("\n");
    detail.append(text);
  }

  function plot(id, filtered, axis, lhs, rhs) {
    const canvas = byId(id);
    canvas.replaceChildren();
    const left = 64, right = 495, top = 24, bottom = 341;
    let max = 1;
    for (const sample of filtered) max = Math.max(max, sample[lhs], sample[rhs]);
    const x = (value) => left + value / max * (right - left);
    const y = (value) => bottom - value / max * (bottom - top);
    const diag = svg("line", {x1:left, y1:bottom, x2:right, y2:top, stroke:"#9ab1c4", "stroke-dasharray":"5 5"});
    canvas.append(diag);
    for (let i = 0; i <= 4; i++) {
      const value = max * i / 4;
      const tick = svg("text", {x:x(value), y:bottom + 19, fill:"#b9cbd8", "font-size":11, "text-anchor":"middle"});
      tick.textContent = Number(value.toPrecision(3));
      canvas.append(tick);
      const vertical = svg("text", {x:left - 9, y:y(value) + 4, fill:"#b9cbd8", "font-size":11, "text-anchor":"end"});
      vertical.textContent = Number(value.toPrecision(3));
      canvas.append(vertical);
    }
    const xlabel = svg("text", {x:(left + right) / 2, y:391, fill:"#d8eaf2", "text-anchor":"middle", "font-size":13});
    xlabel.textContent = `Codegen+Yosys/ABC ${axis}`;
    canvas.append(xlabel);
    const ylabel = svg("text", {x:18, y:192, transform:"rotate(-90 18 192)", fill:"#d8eaf2", "text-anchor":"middle", "font-size":13});
    ylabel.textContent = `G8r+ABC ${axis}`;
    canvas.append(ylabel);
    for (const sample of filtered) {
      const classification = outcome(sample);
      const point = svg("circle", {cx:x(sample[rhs]), cy:y(sample[lhs]), r:5, fill:classification === "win" ? "#9ceec0" : classification === "loss" ? "#ff9bb0" : "#f5d28d", tabindex:0, role:"button"});
      point.setAttribute("aria-label", `${sample.origins[0]?.function || sample.sample_id}: ${sample[lhs]} vs ${sample[rhs]} ${axis}`);
      point.addEventListener("click", () => show(sample));
      point.addEventListener("keydown", (event) => { if (event.key === "Enter" || event.key === " ") { event.preventDefault(); show(sample); } });
      canvas.append(point);
    }
  }

  function render() {
    const kind = byId("kind").value, result = byId("outcome").value;
    const opsText = byId("max-ops").value;
    const maxOps = opsText === "" ? null : Number(opsText);
    const filtered = samples.filter((sample) =>
      (!kind || sample.kind === kind) && (!result || outcome(sample) === result) &&
      (maxOps === null || sample.ir_op_count !== null && sample.ir_op_count <= maxOps));
    byId("shown").textContent = `Showing ${filtered.length} of ${samples.length} paired samples. Green wins both metrics, red loses both, yellow ties or mixes. Diagonal means equal results.`;
    plot("nodes", filtered, "AND nodes", "g8r_nodes", "yosys_nodes");
    plot("depth", filtered, "levels", "g8r_depth", "yosys_depth");
    const rows = byId("rows");
    rows.replaceChildren();
    for (const sample of filtered) {
      const tr = document.createElement("tr");
      tr.dataset.id = sample.sample_id;
      const values = [sample.kind, `${sample.origins[0]?.file || "?"} :: ${sample.origins[0]?.function || "?"}`,
        sample.ir_op_count ?? "?", `${sample.g8r_nodes} / ${sample.g8r_depth}`,
        `${sample.yosys_nodes} / ${sample.yosys_depth}`, outcome(sample)];
      for (const value of values) {
        const td = document.createElement("td");
        td.textContent = value;
        tr.append(td);
      }
      tr.lastChild.className = outcome(sample);
      tr.addEventListener("click", () => show(sample));
      rows.append(tr);
    }
  }
  for (const id of ["kind", "outcome", "max-ops"]) byId(id).addEventListener("input", render);
  render();

  const failures = byId("failures");
  if (coverage.scanned_files < coverage.input_files) {
    const line = document.createElement("p");
    line.textContent = `${coverage.input_files - coverage.scanned_files} source files were not examined because of --max-files.`;
    failures.append(line);
  }
  if (coverage.function_limit_reached) label(failures, "Concrete function limit reached; some discovered functions were not converted. ");
  if (!coverage.ingest_failures.length && !coverage.comparison_failures.length) label(failures, "No recorded failures.");
  for (const failure of coverage.ingest_failures) {
    const line = document.createElement("p");
    line.textContent = `Ingest: ${failure.source_relpath}${failure.source_fn ? ` :: ${failure.source_fn}` : ""}: ${failure.stage}: ${failure.reason}`;
    failures.append(line);
  }
  for (const failure of coverage.comparison_failures) {
    const line = document.createElement("p");
    line.textContent = `Comparison: ${failure.cone ?? "unknown"}: ${failure.reason ?? "failed"}${failure.stages?.length ? ` (${failure.stages.join(", ")})` : ""}`;
    failures.append(line);
  }
})();
