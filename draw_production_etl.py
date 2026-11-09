#!/usr/bin/env python3
"""Draw "Production Token Migration ETL.png" (black title tab, cylinders for stores, rounded
boxes for services, a parallelogram for the staging transform, a yellow note, a rules table).
Every arrow is made of straight horizontal and vertical runs.

    python3 draw_production_etl.py
"""
from pathlib import Path

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt
from matplotlib.patches import Ellipse, FancyArrowPatch, FancyBboxPatch, Polygon, Rectangle

OUT = Path(__file__).resolve().parent / "Production Token Migration ETL.png"
W, H = 19.81, 11.87
LW = 2.0
FONT = "DejaVu Sans"
TEAL, TEAL_FILL = "#0f7f86", "#E8F4F4"

fig = plt.figure(figsize=(W, H), dpi=150)
ax = fig.add_axes([0, 0, 1, 1])
ax.set_xlim(0, W)
ax.set_ylim(0, H)
ax.axis("off")


def text(x, y, s, size=12, weight="normal", style="normal", color="black", ha="center", va="center"):
    ax.text(x, y, s, fontsize=size, fontweight=weight, fontstyle=style, color=color, ha=ha, va=va,
            family=FONT, linespacing=1.25, zorder=6)


def cylinder(cx, cy, w, h, label, size=12.5):
    e = 0.15
    ax.add_patch(Ellipse((cx, cy - h / 2), w, 2 * e, facecolor="white", edgecolor="black", lw=LW, zorder=3))
    ax.add_patch(Rectangle((cx - w / 2, cy - h / 2), w, h, facecolor="white", edgecolor="none", zorder=3.1))
    ax.plot([cx - w / 2, cx - w / 2], [cy - h / 2, cy + h / 2], color="black", lw=LW, zorder=3.2)
    ax.plot([cx + w / 2, cx + w / 2], [cy - h / 2, cy + h / 2], color="black", lw=LW, zorder=3.2)
    ax.add_patch(Ellipse((cx, cy + h / 2), w, 2 * e, facecolor="white", edgecolor="black", lw=LW, zorder=3.3))
    text(cx, cy, label, size=size)


def rbox(cx, cy, w, h, label, size=12.5, weight="normal", fill="white", edge="black", dashed=False):
    ax.add_patch(FancyBboxPatch((cx - w / 2, cy - h / 2), w, h, boxstyle="round,pad=0,rounding_size=0.1",
                                facecolor=fill, edgecolor=edge, lw=LW, linestyle="--" if dashed else "-",
                                zorder=3))
    text(cx, cy, label, size=size, weight=weight)


def para(cx, cy, w, h, label, size=12.5, s=0.6):
    ax.add_patch(Polygon([(cx - w / 2, cy - h / 2), (cx + w / 2 - s, cy - h / 2), (cx + w / 2, cy + h / 2),
                          (cx - w / 2 + s, cy + h / 2)], closed=True, facecolor="white", edgecolor="black",
                         lw=LW, zorder=3))
    text(cx, cy, label, size=size)


def note(x, y, w, h, label, size=11):
    ax.add_patch(Rectangle((x + 0.05, y - 0.05), w, h, facecolor="#E6E6E6", edgecolor="none", zorder=2))
    ax.add_patch(Rectangle((x, y), w, h, facecolor="#FDDC82", edgecolor="#C9A227", lw=1.2, zorder=3))
    text(x + w / 2, y + h / 2, label, size=size)


def route(pts, label=None, lx=None, ly=None, color="black", both=False, size=11):
    """An arrow along straight horizontal/vertical runs through pts; the head is on the last point."""
    for (x1, y1), (x2, y2) in zip(pts, pts[1:]):
        assert x1 == x2 or y1 == y2, "arrows run straight"
    for (x1, y1), (x2, y2) in zip(pts[:-2], pts[1:-1]):
        ax.plot([x1, x2], [y1, y2], color=color, lw=1.6, zorder=5, solid_capstyle="butt")
    style = "<|-|>" if both else "-|>"
    ax.add_patch(FancyArrowPatch(pts[-2], pts[-1], arrowstyle=style, mutation_scale=18, lw=1.6, color=color,
                                 zorder=5, shrinkA=0, shrinkB=0))
    if label:
        text(lx, ly, label, size=size, weight="bold", color=color)


# frame, title tab, subtitle
ax.add_patch(FancyBboxPatch((0.4, 0.5), 19.02, 10.52, boxstyle="round,pad=0,rounding_size=0.18",
                            facecolor="white", edgecolor="black", lw=LW, zorder=1))
ax.add_patch(FancyBboxPatch((0.55, 10.26), 4.6, 0.88, boxstyle="round,pad=0,rounding_size=0.1",
                            facecolor="black", edgecolor="black", zorder=2))
text(0.77, 10.70, "Production Token Migration ETL", size=16, weight="bold", color="white", ha="left")
text(0.58, 10.06, "any merchant, config-driven", size=12.5, style="italic", color="#555555", ha="left")

# extraction row
y1 = 8.43
cylinder(1.78, y1, 2.22, 1.55, "Merchant\nSource(s)")
route([(2.89, y1), (3.88, y1)], "{GET}", 3.38, y1 + 0.24)
rbox(5.10, y1, 2.45, 1.43, "Extraction\nAdapter", weight="bold")
route([(6.32, y1), (7.78, y1)], "{GET}", 7.05, y1 + 0.24)
cylinder(8.98, y1, 2.4, 1.55, "GCS Bucket\nraw/<merchant>/")

# sink column
cylinder(8.98, 1.91, 2.4, 1.55, "Merchant\nTarget System")
rbox(8.98, 4.55, 2.45, 1.43, "Sink\nAdapter", weight="bold")
route([(8.98, y1 - 0.775 - 0.15), (8.98, 4.55 + 0.715)], "{POST}", 9.55, 6.0)
route([(8.98, 4.55 - 0.715), (8.98, 1.91 + 0.775 + 0.15)], "{POST}", 9.55, 3.25)

note(1.5, 5.10, 3.05, 2.04, "Extraction and sink are\npluggable per merchant --\nswap the adapter config,\n"
     "not the pipeline code.\nDataflow / SQL opt in only\nwhere a merchant needs it.")

# staging panel
ax.add_patch(FancyBboxPatch((11.57, 0.86), 7.58, 9.98, boxstyle="round,pad=0,rounding_size=0.15",
                            facecolor="white", edgecolor="black", lw=LW, zorder=1.5))
sx, sy, sw, sh, ss = 13.6, 7.23, 3.9, 1.66, 0.6
para(sx, sy, sw, sh, "Staging Service\n(transform + reconcile)", s=ss)
left_edge = lambda y: sx - sw / 2 + ss * (y - (sy - sh / 2)) / sh  # x of the slanted left side at height y

rbox(sx, 9.54, 2.63, 1.15, "Mapping Config\nconfigs/<merchant>.json", size=11, fill=TEAL_FILL, edge=TEAL)
rbox(17.45, 9.54, 2.82, 1.15, "Gemini Agent\n(Cloud Shell / CLI)", size=11, fill=TEAL_FILL, edge=TEAL, dashed=True)
route([(17.45 - 1.41, 9.54), (sx + 1.315, 9.54)], "proposes", 15.5, 9.78, color=TEAL)
route([(sx, 9.54 - 0.575), (sx, sy + sh / 2)], "drives,\nhuman-approved", sx + 1.0, 8.5, color=TEAL, size=10)

rbox(17.65, sy, 2.63, 1.43, "Reconciliation\nStore", weight="bold")
route([(sx + sw / 2 - ss / 2, sy), (17.65 - 1.315, sy)], "{POST}", 15.85, sy + 0.26, both=True)

# GCS <-> staging
route([(10.18, 7.85), (left_edge(7.85), 7.85)], "reads\ntransformed/", 10.95, 8.3, size=10.5)
route([(left_edge(6.75), 6.75), (9.7, 6.75), (9.7, 7.49)], "writes\ncleaned/", 10.75, 6.35, size=10.5)

# mapping rules table
tx, top, cw, rh = 12.13, 5.20, [2.71, 3.76], 0.8
rows = [["Rule type", "Example"],
        ["renames", "CustomerName → name"],
        ["expansions.copy_then_blank", "UniqueId → card.id, card.number"],
        ["expansions.split_date", "Expiry → card.exp_year / _month"],
        ["repairs.zero_pad_if_length", "AccountNumberLast4"]]
for i, row in enumerate(rows):
    y = top - rh * (i + 1)
    x = tx
    for j, val in enumerate(row):
        ax.add_patch(Rectangle((x, y), cw[j], rh, facecolor="#EEEEEE" if i == 0 else "white", edgecolor="black",
                               lw=1.5 if i == 0 else 1.0, zorder=3))
        text(x + 0.18, y + rh / 2, val, size=10, weight="bold" if i == 0 else "normal", ha="left")
        x += cw[j]

fig.savefig(OUT, dpi=150, facecolor="white")
print("wrote", OUT)
