# Package the supplied NatGlobe artwork for the iOS home screen. Neither the
# public ACARS icon nor the passenger booking brand is replaced.
$ErrorActionPreference = 'Stop'
Add-Type -AssemblyName System.Drawing
$publicDir = Join-Path $PSScriptRoot '../public'
$logo = [Drawing.Image]::FromFile((Resolve-Path (Join-Path $publicDir 'pilot-natglobe-logo.png')))
try {
  foreach ($size in @(180,192,512)) {
    $icon = New-Object Drawing.Bitmap $size,$size
    $g = [Drawing.Graphics]::FromImage($icon)
    try {
      $g.Clear([Drawing.Color]::White)
      $g.InterpolationMode = [Drawing.Drawing2D.InterpolationMode]::HighQualityBicubic
      $g.PixelOffsetMode = [Drawing.Drawing2D.PixelOffsetMode]::HighQuality
      # Trim only the white margins of the 1414 x 517 master; keep both lines of type.
      $src = [Drawing.Rectangle]::new(208,10,1024,486)
      $w = [int]($size * .88); $h = [int]($w * $src.Height / $src.Width)
      $dest = [Drawing.Rectangle]::new([int](($size-$w)/2),[int](($size-$h)/2),$w,$h)
      $g.DrawImage($logo,$dest,$src,[Drawing.GraphicsUnit]::Pixel)
      $icon.Save((Join-Path $publicDir "pilot-natglobe-$size.png"),[Drawing.Imaging.ImageFormat]::Png)
    } finally { $g.Dispose(); $icon.Dispose() }
  }
} finally { $logo.Dispose() }
