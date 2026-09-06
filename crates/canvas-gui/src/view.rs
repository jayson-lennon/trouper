//! Minimal pure geometry shared by layout, hit-testing, and rendering.
//!
//! No bevy/glam here: render.rs converts `Vec2` → `bevy::math::Vec2` at
//! the boundary. Coordinate conventions live exactly once, in this
//! module's docs:
//!
//! - **World space** is y-up (bevy-native), origin at the scene center.
//! - **Screen space** is pixel coordinates as winit reports them
//!   (y-down, origin top-left).
//! - The y-flip between the two happens ONLY in
//!   [`ViewTransform::screen_to_world`] / [`ViewTransform::world_to_screen`].

use std::ops::{Add, Div, Mul, Sub};

/// A 2D vector in either world or screen space (documented per use).
#[derive(Debug, Clone, Copy, PartialEq, Default)]
pub struct Vec2 {
    /// Horizontal component.
    pub x: f32,
    /// Vertical component.
    pub y: f32,
}

impl Vec2 {
    /// The zero vector.
    pub const ZERO: Self = Self { x: 0.0, y: 0.0 };

    /// Builds a vector.
    #[must_use]
    pub const fn new(x: f32, y: f32) -> Self {
        Self { x, y }
    }

    /// Both components set to `v`.
    #[must_use]
    pub const fn splat(v: f32) -> Self {
        Self { x: v, y: v }
    }

    /// Componentwise minimum.
    #[must_use]
    pub fn min(self, other: Self) -> Self {
        Self::new(self.x.min(other.x), self.y.min(other.y))
    }

    /// Componentwise maximum.
    #[must_use]
    pub fn max(self, other: Self) -> Self {
        Self::new(self.x.max(other.x), self.y.max(other.y))
    }

    /// Euclidean length.
    #[must_use]
    pub fn length(self) -> f32 {
        (self.x * self.x + self.y * self.y).sqrt()
    }

    /// True when both components are within `eps`.
    #[must_use]
    pub fn approx_eq(self, other: Self, eps: f32) -> bool {
        (self.x - other.x).abs() <= eps && (self.y - other.y).abs() <= eps
    }
}

impl Add for Vec2 {
    type Output = Self;
    fn add(self, rhs: Self) -> Self {
        Self::new(self.x + rhs.x, self.y + rhs.y)
    }
}

impl Sub for Vec2 {
    type Output = Self;
    fn sub(self, rhs: Self) -> Self {
        Self::new(self.x - rhs.x, self.y - rhs.y)
    }
}

impl Mul<f32> for Vec2 {
    type Output = Self;
    fn mul(self, rhs: f32) -> Self {
        Self::new(self.x * rhs, self.y * rhs)
    }
}

impl Div<f32> for Vec2 {
    type Output = Self;
    fn div(self, rhs: f32) -> Self {
        Self::new(self.x / rhs, self.y / rhs)
    }
}

/// Zoom bounds (world units per pixel range).
pub const MIN_ZOOM: f32 = 0.05;
/// Zoom bounds (world units per pixel range).
pub const MAX_ZOOM: f32 = 20.0;

/// The camera's 2D transform: world point at the viewport center
/// (`pan`) and world-units-per-pixel (`zoom`).
#[derive(Debug, Clone, Copy, PartialEq, bevy::ecs::resource::Resource)]
pub struct ViewTransform {
    /// World coordinates at the viewport center.
    pub pan: Vec2,
    /// World units per pixel (bigger = zoomed out).
    pub zoom: f32,
}

impl Default for ViewTransform {
    fn default() -> Self {
        Self {
            pan: Vec2::ZERO,
            zoom: 1.0,
        }
    }
}

impl ViewTransform {
    /// Screen pixels (y-down) → world point (y-up). The single
    /// coordinate handedness flip in the codebase happens here.
    #[must_use]
    pub fn screen_to_world(&self, screen_px: Vec2, viewport_px: Vec2) -> Vec2 {
        let offset = (screen_px - viewport_px * 0.5) / self.zoom;
        Vec2::new(self.pan.x + offset.x, self.pan.y - offset.y)
    }

    /// World point (y-up) → screen pixels (y-down); inverse of
    /// [`ViewTransform::screen_to_world`].
    #[must_use]
    pub fn world_to_screen(&self, world: Vec2, viewport_px: Vec2) -> Vec2 {
        let offset = world - self.pan;
        Vec2::new(
            viewport_px.x * 0.5 + offset.x * self.zoom,
            viewport_px.y * 0.5 - offset.y * self.zoom,
        )
    }

    /// Zooms by `factor` keeping the world point under `screen_px`
    /// pinned to the cursor (clamped to [`MIN_ZOOM`]..[`MAX_ZOOM`]).
    #[must_use]
    pub fn zoom_at(&self, screen_px: Vec2, viewport_px: Vec2, factor: f32) -> Self {
        let zoom = (self.zoom * factor).clamp(MIN_ZOOM, MAX_ZOOM);
        let anchor = self.screen_to_world(screen_px, viewport_px);
        let offset = (screen_px - viewport_px * 0.5) / zoom;
        Self {
            pan: Vec2::new(anchor.x - offset.x, anchor.y + offset.y),
            zoom,
        }
    }

    /// Pans by screen-pixel delta (converted through zoom).
    #[must_use]
    pub fn pan_by_px(&self, delta_px: Vec2) -> Self {
        Self {
            pan: self.pan + Vec2::new(delta_px.x, -delta_px.y) / self.zoom,
            zoom: self.zoom,
        }
    }
}

/// The parameter range `[t0, t1]` (within `0.0..=1.0`) where the
/// segment `a → b` lies inside the axis-aligned box `min..max`
/// (parametric Liang–Barsky). Returns `None` when the segment never
/// enters the box.
fn clip_param_range(a: Vec2, b: Vec2, min: Vec2, max: Vec2) -> Option<(f32, f32)> {
    let d = b - a;
    let mut t0 = 0.0_f32;
    let mut t1 = 1.0_f32;
    let boundaries = [
        (-d.x, a.x - min.x),
        (d.x, max.x - a.x),
        (-d.y, a.y - min.y),
        (d.y, max.y - a.y),
    ];
    for (p, q) in boundaries {
        if p.abs() < f32::EPSILON {
            if q < 0.0 {
                return None;
            }
        } else {
            let t = q / p;
            if p < 0.0 {
                t0 = t0.max(t);
            } else {
                t1 = t1.min(t);
            }
            if t0 > t1 {
                return None;
            }
        }
    }
    Some((t0, t1))
}

/// Clips the center-to-center segment between two boxes so it starts
/// where it exits the first box and ends where it enters the second
/// (the edge rendering geometry). Returns `None` when the boxes overlap
/// so deeply that no exit-before-entry segment exists.
#[must_use]
pub fn clip_segment_between_boxes(
    center_a: Vec2,
    size_a: Vec2,
    center_b: Vec2,
    size_b: Vec2,
) -> Option<(Vec2, Vec2)> {
    let half_a = size_a * 0.5;
    let half_b = size_b * 0.5;
    let direction = center_b - center_a;
    let (_, exit_a) = clip_param_range(center_a, center_b, center_a - half_a, center_a + half_a)?;
    let (enter_b, _) = clip_param_range(center_a, center_b, center_b - half_b, center_b + half_b)?;
    if exit_a > enter_b {
        return None;
    }
    Some((
        center_a + direction * exit_a,
        center_a + direction * enter_b,
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn screen_world_roundtrip() {
        // Given several pan/zoom/viewport combinations.
        let cases = [
            (Vec2::new(0.0, 0.0), 1.0, Vec2::new(1280.0, 720.0)),
            (Vec2::new(-350.0, 120.0), 0.4, Vec2::new(1920.0, 1080.0)),
            (Vec2::new(900.0, -60.0), 2.5, Vec2::new(800.0, 600.0)),
            (Vec2::new(12.5, 3.25), 0.05, Vec2::new(640.0, 480.0)),
        ];
        for (pan, zoom, viewport) in cases {
            let view = ViewTransform { pan, zoom };

            // When a screen point round-trips through world space.
            let screen = Vec2::new(517.0, 342.0);
            let back = view.world_to_screen(view.screen_to_world(screen, viewport), viewport);

            // Then it lands on itself (the y-flip cancels out).
            assert!(back.approx_eq(screen, 1e-3), "{back:?} != {screen:?}");
        }
    }

    #[test]
    fn zoom_keeps_cursor_world_point_fixed() {
        // Given a view and a cursor position.
        let viewport = Vec2::new(1280.0, 720.0);
        let view = ViewTransform {
            pan: Vec2::new(40.0, -30.0),
            zoom: 1.0,
        };
        let cursor = Vec2::new(1000.0, 200.0);
        let anchored = view.screen_to_world(cursor, viewport);

        // When zooming in and out at that cursor.
        let zoomed_in = view.zoom_at(cursor, viewport, 2.0);
        let zoomed_out = view.zoom_at(cursor, viewport, 0.5);

        // Then the world point under the cursor is unchanged in both.
        assert!(
            zoomed_in
                .screen_to_world(cursor, viewport)
                .approx_eq(anchored, 1e-3)
        );
        assert!(
            zoomed_out
                .screen_to_world(cursor, viewport)
                .approx_eq(anchored, 1e-3)
        );
        // And the zoom clamp holds at the extremes.
        assert_eq!(view.zoom_at(cursor, viewport, 1e6).zoom, MAX_ZOOM);
        assert_eq!(view.zoom_at(cursor, viewport, 1e-6).zoom, MIN_ZOOM);
    }

    #[test]
    fn clip_segment_touches_both_rects() {
        // Given two non-overlapping boxes and their center segment.
        let a = Vec2::new(0.0, 0.0);
        let size_a = Vec2::new(200.0, 80.0);
        let b = Vec2::new(500.0, 200.0);
        let size_b = Vec2::new(200.0, 80.0);

        // When clipping the segment to both borders.
        let (start, end) =
            clip_segment_between_boxes(a, size_a, b, size_b).expect("segment exists");

        // Then the start lies on box A's border (x at the right edge).
        assert!(start.approx_eq(Vec2::new(100.0, 40.0), 1e-4));
        // And the end lies on box B's border (x at the left edge).
        assert!(end.approx_eq(Vec2::new(400.0, 160.0), 1e-4));
    }

    #[test]
    fn clip_segment_is_none_when_boxes_overlap() {
        // Given two identical boxes.
        let center = Vec2::new(10.0, 10.0);
        let size = Vec2::new(100.0, 100.0);

        // When clipping the degenerate segment between them.
        let clipped = clip_segment_between_boxes(center, size, center, size);

        // Then there is no border-to-border segment.
        assert!(clipped.is_none());
    }
}
