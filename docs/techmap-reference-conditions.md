# Reference conditions for technology mapping

The reference-flop convention defines a reproducible output load and primary-input slew for
combinational technology-mapping comparisons. Each design kit supplies a pinned reference cell
and characterization profile. The convention applies across kits; its numerical values depend
on the selected kit and corner.

This document records the selected convention for future measurements. The current runner and
published measurements still use **10 ps primary-input slew and 1 fF output load**. Adopting the
convention requires a separately identified mapping/evaluation configuration and new results.

## Convention, version 1

1. Select a plain, non-scan D flip-flop with the kit's regular threshold voltage and an ordinary
   X2 output drive, where available. Record an explicit alternative when the kit uses different
   names or drive options. X2 is a library drive designation; read the actual data-pin capacitance
   rather than inferring it from that designation.
2. Pin the analysis corner and exact Liberty revision. Identify the data input, clock input,
   chosen output, and clock-to-output timing arc, including its edge and any condition. The
   chosen output may be Q or QN.
3. Let `C_D` be the data input's Liberty scalar `capacitance`. If the scalar is absent and both
   `rise_capacitance` and `fall_capacitance` are present, use their larger value and record that
   selection. A profile lacking either complete representation needs an explicitly specified
   alternative.
4. Set every module output's external load to **`C_D`**, representing one reference-flop data
   input. Wiring capacitance is zero in this convention.
5. Calibrate the launch slew using the reference flop driving **`4 * C_D`**. This four-data-input
   load is distinct from the one-data-input load at module outputs. Select and record a reference
   clock-input slew for the kit; it remains an explicit calibration parameter.
6. At that clock slew and calibration load, interpolate the selected clock-to-output arc's
   `rise_transition` and `fall_transition` tables. Set the scalar primary-input slew to the
   **larger output transition**. Use the transition tables for slew, not the propagation-delay
   tables `cell_rise` and `cell_fall`.

Use linear interpolation along each table axis (bilinear interpolation between four grid
points), identifying the axes from the template variables and respecting arc-specific index
overrides. Version 1 uses two-dimensional transition tables; other timing representations need
an explicitly specified profile. Both coordinates must be within the
characterized range of both transition tables. A kit needing a different reference profile or
an extrapolation rule must specify that explicitly; do not silently clamp the calibration point.

Interpret values using the Liberty time and capacitance units. Record the applicable slew
thresholds, including cell or pin overrides, and use the same transition definition in evaluation.
For example, a 10–90% transition and a 20–80% transition have different numerical meanings.
The ASAP7 clock calibration value below is a kit-specific choice, not a universal clock slew.

The resulting metric remains nominal combinational delay: the reference flop defines electrical
boundary conditions. Its clock-to-output propagation delay and receiver setup time are not added
to the combinational path. Comparisons must share the resolved evaluator conditions and report
the electrical assumptions used by each mapper separately.

## ASAP7 RVT SS profile

The reference comes from the sequential library at the same upstream revision and corner as
the comparison's combinational libraries:

- Repository: [siliconcompiler/lambdapdk](https://github.com/siliconcompiler/lambdapdk).
- Revision: `720a7e3d4c765496884dd58159152a142c2e769b`.
- Source: [`asap7sc7p5t_SEQ_RVT_SS_nldm.lib.gz`](https://github.com/siliconcompiler/lambdapdk/blob/720a7e3d4c765496884dd58159152a142c2e769b/lambdapdk/asap7/libs/asap7sc7p5t_rvt/nldm/asap7sc7p5t_SEQ_RVT_SS_nldm.lib.gz).
- SHA-256 of the decompressed Liberty bytes:
  `c6a6d9ad22bfb1bc9cf1c4ac66d09ce575ca18648ce4e50597d92174bc290d78`.

| Profile item | Value |
| --- | --- |
| Corner | RVT, SS, 0.63 V, 100 °C |
| Reference cell | `DFFHQNx2_ASAP7_75t_R` |
| Data / clock / output pins | `D` / `CLK` / `QN` |
| Clock-to-output arc | `related_pin: CLK`, rising clock edge, unconditional |
| Liberty time / capacitance units | 1 ps / 1 fF |
| Slew thresholds | 10–90%, rising and falling |
| `D.capacitance`, hence module-output load | **0.52508 fF** |
| Four-data-input calibration load | **2.10032 fF** |
| Reference clock-input slew | **10 ps** |
| Interpolated output rise transition | 38.6053481889 ps |
| Interpolated output fall transition | 43.9069078667 ps |
| Selected primary-input slew | **43.9069078667 ps**, approximately 44 ps |

At the 10 ps clock-slew row, the bracketing load entries are:

| Output load (fF) | `rise_transition` (ps) | `fall_transition` (ps) |
| --- | --- | --- |
| 1.44 | 33.1838 | 39.2478 |
| 2.88 | 45.0069 | 49.4082 |

For either transition, `t(C) = t(1.44) + w * (t(2.88) - t(1.44))`, where
`w = (2.10032 - 1.44) / (2.88 - 1.44) = 0.458555555556`.
The clock slew is already on a table grid point, so this example needs interpolation only along
the load axis. The selected clock-slew and load coordinates are inside both tables' ranges.

The sequential library supplies the reference values only; the combinational mapper cell set
remains the four libraries identified in [the ASAP7 library summary](../third_party/asap7/VENDORED.md).

## Recording an adopted profile

When this convention is implemented, persist the profile and resolved settings in typed protobuf
evidence: convention version; source revision and digest; reference cell, pins, arc and corner;
capacitance selection; calibration fanout and clock slew; transition thresholds and interpolation
rule; and resolved input slew and output load. Include the settings and recipe identity in the
mapping and evaluation cache identities.

Preserve existing results under their original configuration. A comparison using this profile
must use the resolved settings consistently in its common evaluator and identify each mapper's
optimization assumptions. Reporting the original 10 ps / 1 fF measurements as profile measurements
would change their meaning without recomputation.
