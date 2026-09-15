"""
OVID Scheduler Batch Completion Email Notifier
==============================================

Sends an HTML email summarising the outcome of a scheduler batch:
    - Batch ID, status, duration shown in a hero block
    - Two-column metadata grid (database, server, stages, instance, etc.)
    - Per-table status and Snowflake row counts (CDM tables and, when the
      scheduler ran the post-ETL raw_upload stage, raw files too)
    - ERROR / FAILURE lines collected from the OVID log file

Design notes
------------
The styling is intentionally minimal in the spirit of modern transactional
email (Linear / Stripe / Vercel):

* System font stack -- no remote web fonts, renders crisply on every
  email client (Apple Mail, Outlook, Gmail web/mobile).
* Muted neutral palette with a single accent colour for the status pill,
  derived from the official UF brand identity (signature orange + academic
  blue).  Body text is slate grey on white; borders are 1px hairlines.
* Compact pill-shaped status badge instead of the previous full-width
  banner.  The pill is the only saturated colour on the page so it pops
  in the inbox preview without shouting.
* Tables Processed: zebra rows with a coloured-dot status indicator and
  right-aligned monospace counts; no heavy borders.
* Errors: left-rule callout with line numbers in a dim mono font.
* Slim signed footer with the log file path and a single hairline rule.
"""

import datetime
import logging
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from html import escape as html_escape
from typing import Any, Optional

logger = logging.getLogger("ovid")


# ─── Palette ────────────────────────────────────────────────────────────────
# Neutral foundation -- everything that is not a status accent uses these.
_INK             = "#111827"   # Primary text (slate near-black)
_INK_MUTED       = "#4b5563"   # Secondary text (mid grey)
_INK_DIM         = "#9ca3af"   # Tertiary text / labels (dim grey)
_RULE            = "#e5e7eb"   # Hairline borders
_RULE_SOFT       = "#f3f4f6"   # Zebra-stripe row tint
_SURFACE         = "#ffffff"   # Page background

# UF brand accents -- used sparingly.  See https://identity.ufl.edu/brand/colors/
_UF_ORANGE       = "#FA4616"   # Signature orange (PROCESSED pill)
_UF_BLUE         = "#0021A5"   # Academic blue (uploaded status, headings)
_UF_BLUE_DEEP    = "#001A5C"   # Deep navy for emphasis links

# Universal status colours -- distinct from UF brand colours so the
# semantics still read clearly across colour-blind viewers.
_STATUS_RED      = "#dc2626"   # Errors
_STATUS_AMBER    = "#d97706"   # Warnings / processing / skipped
_STATUS_GREEN    = "#15803d"   # Uploaded (muted forest, not the eye-watering CSS green)
_STATUS_GREY     = "#6b7280"   # Neutral / not_found

# Modern system font stack -- no web-font dependency, renders natively on
# every major email client.
_FONT_STACK = (
    '-apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, '
    '"Helvetica Neue", Arial, sans-serif'
)
_MONO_FONT = (
    '"SF Mono", "Cascadia Mono", "JetBrains Mono", "Consolas", '
    '"Courier New", monospace'
)


# Pill colour pairs (background, foreground) for each status word.  The
# pill is the single saturated visual on the page; everything else is
# neutral, so the pill carries the at-a-glance signal.
_STATUS_PILL = {
    "PROCESSED":  ("#fff1ec", _UF_ORANGE),     # warm orange tint + signature orange ink
    "ERROR":      ("#fee2e2", _STATUS_RED),     # soft red tint + red ink
    "PROCESSING": ("#fef3c7", _STATUS_AMBER),   # amber tint + amber ink
    "NEW":        ("#f3f4f6", _STATUS_GREY),    # neutral
}


class BatchNotifier:
    """Send batch completion notifications for the Ovid scheduler."""

    def __init__(
        self,
        smtp_host: str,
        smtp_port: int,
        from_address: str,
        recipients: list[str],
        subject_prefix: str = "[OVID Scheduler]",
        timeout_seconds: int = 30,
    ) -> None:
        """Initialize the notifier.

        Args:
            smtp_host: SMTP relay hostname.
            smtp_port: SMTP relay port (typically 25).
            from_address: The ``From:`` header for outgoing emails.
            recipients: List of recipient email addresses.
            subject_prefix: Prefix prepended to every subject line.
            timeout_seconds: SMTP connection timeout.
        """
        self.smtp_host = smtp_host
        self.smtp_port = smtp_port
        self.from_address = from_address
        self.recipients = list(recipients)
        self.subject_prefix = subject_prefix
        self.timeout_seconds = timeout_seconds

    # ─── Public API ────────────────────────────────────────────────────────

    def send_batch_completion(self, batch_info: dict[str, Any]) -> bool:
        """Send an HTML email summarising a completed batch.

        Args:
            batch_info: Dict containing batch metadata. Expected keys:
                batch_id, partner, folder, status, processing_start,
                processing_end, duration, host, instance_id, tables,
                errors, log_file_path, and (optionally) raw_upload +
                raw_tables when the scheduler ran the post-ETL raw_upload
                stage.

        Returns:
            True if the email was sent successfully, False otherwise.
        """
        if not self.recipients:
            logger.warning("No recipients configured, skipping batch email")
            return False

        try:
            subject = self._build_subject(batch_info)
            html_body = self._render_html(batch_info)
            text_body = self._render_text(batch_info)
            return self._send(subject, html_body, text_body)
        except Exception:
            logger.error("Failed to build or send batch completion email",
                         exc_info=True)
            return False

    # ─── Internal helpers ──────────────────────────────────────────────────

    def _send(self, subject: str, html_body: str, text_body: str) -> bool:
        """Send a multipart alternative email (plain-text + HTML).

        Args:
            subject: Email subject line.
            html_body: HTML body.
            text_body: Plain-text fallback body.

        Returns:
            True if sent successfully, False otherwise.
        """
        try:
            msg = MIMEMultipart("alternative")
            msg["Subject"] = subject
            msg["From"] = self.from_address
            msg["To"] = ", ".join(self.recipients)

            msg.attach(MIMEText(text_body, "plain", "utf-8"))
            msg.attach(MIMEText(html_body, "html", "utf-8"))

            with smtplib.SMTP(self.smtp_host, self.smtp_port,
                              timeout=self.timeout_seconds) as server:
                server.sendmail(self.from_address, self.recipients,
                                msg.as_string())

            logger.info(
                f"Batch completion email sent: '{subject}' to "
                f"{self.recipients} via {self.smtp_host}:{self.smtp_port}"
            )
            return True

        except Exception as e:
            logger.error(
                f"Failed to send batch completion email via "
                f"{self.smtp_host}:{self.smtp_port}: {e}",
                exc_info=True,
            )
            return False

    def _build_subject(self, batch_info: dict[str, Any]) -> str:
        """Build the subject line for the batch completion email."""
        status = batch_info.get("status", "UNKNOWN")
        batch_id = batch_info.get("batch_id", "unknown_batch")
        duration = batch_info.get("duration")
        duration_str = _format_duration(duration) if duration else "n/a"
        return (
            f"{self.subject_prefix} {status} — {batch_id} — {duration_str}"
        )

    # ─── Section renderers ─────────────────────────────────────────────────

    def _render_hero(self, batch_info: dict[str, Any]) -> str:
        """Render the hero block: status pill + batch_id + duration."""
        status = str(batch_info.get("status", "UNKNOWN"))
        bg, fg = _STATUS_PILL.get(status, ("#f3f4f6", _STATUS_GREY))
        batch_id = batch_info.get("batch_id") or "unknown_batch"
        duration = _format_duration(batch_info.get("duration"))

        pill = (
            f"<span style='display:inline-block;padding:4px 12px;"
            f"border-radius:999px;background:{bg};color:{fg};"
            f"font-size:11px;font-weight:600;letter-spacing:0.08em;"
            f"text-transform:uppercase;font-family:{_FONT_STACK};'>"
            f"{html_escape(status)}</span>"
        )

        return f"""
    <div style='padding:24px 0 16px 0;border-bottom:1px solid {_RULE};
                margin-bottom:24px;'>
      <div style='font-size:11px;letter-spacing:0.12em;text-transform:uppercase;
                  color:{_INK_DIM};font-weight:600;margin-bottom:10px;'>
        OneFlorida+ &middot; OVID Scheduler
      </div>
      <div style='font-size:22px;font-weight:600;color:{_INK};
                  letter-spacing:-0.01em;line-height:1.3;
                  font-family:{_MONO_FONT};margin-bottom:8px;
                  word-break:break-all;'>
        {html_escape(str(batch_id))}
      </div>
      <div style='display:flex;align-items:center;gap:12px;'>
        {pill}
        <span style='color:{_INK_MUTED};font-size:13px;'>
          completed in <strong style='color:{_INK};font-weight:600;'>
          {html_escape(duration)}</strong>
        </span>
      </div>
    </div>"""

    def _render_metadata_grid(self, batch_info: dict[str, Any]) -> str:
        """Render a 2-column metadata grid using a nested table layout.

        Email clients vary wildly in CSS-grid / flexbox support, so the
        grid is built with a plain ``<table>`` having two ``<td>`` cells
        per row, each cell carrying its own label/value pair.  Apple
        Mail, Outlook, and Gmail all render this consistently.
        """
        # Pairs of (label, value, mono?) that fill the grid in row-major order.
        # When the count is odd, the last cell renders empty so the grid
        # stays aligned.
        items: list[tuple[str, Optional[str], bool]] = [
            ("Database",    batch_info.get("database"),    True),
            ("DB Server",   batch_info.get("db_server"),   True),
            ("DB Type",     batch_info.get("db_type"),     False),
            ("Upload Type", batch_info.get("upload_type"), False),
            ("Stages",      batch_info.get("stages"),      False),
            ("Raw Upload",  _format_raw_upload_summary(batch_info), False),
            ("Started",     _fmt_dt(batch_info.get("processing_start")), False),
            ("Ended",       _fmt_dt(batch_info.get("processing_end")),   False),
            ("Instance ID", batch_info.get("instance_id"), True),
        ]

        # Pad to even length so the row-major layout is balanced.
        if len(items) % 2 == 1:
            items.append(("", None, False))

        rows_html: list[str] = []
        for i in range(0, len(items), 2):
            left = self._render_metadata_cell(*items[i])
            right = self._render_metadata_cell(*items[i + 1])
            rows_html.append(f"<tr>{left}{right}</tr>")

        return f"""
    <table role='presentation' cellspacing='0' cellpadding='0'
           style='width:100%;border-collapse:collapse;margin-bottom:28px;'>
      <tbody>
        {''.join(rows_html)}
      </tbody>
    </table>"""

    def _render_metadata_cell(
        self, label: str, value: Optional[Any], mono: bool,
    ) -> str:
        """Render a single label/value cell for the metadata grid."""
        if not label:
            return f"<td style='width:50%;padding:0;'>&nbsp;</td>"

        value_str = str(value) if value not in (None, "") else "—"
        is_placeholder = value_str == "—"
        font_family = _MONO_FONT if mono and not is_placeholder else _FONT_STACK
        value_color = _INK_DIM if is_placeholder else _INK

        return (
            f"<td style='width:50%;padding:10px 16px 10px 0;"
            f"vertical-align:top;'>"
            f"<div style='font-size:11px;letter-spacing:0.08em;"
            f"text-transform:uppercase;color:{_INK_DIM};font-weight:600;"
            f"margin-bottom:4px;font-family:{_FONT_STACK};'>"
            f"{html_escape(label)}</div>"
            f"<div style='font-size:13px;color:{value_color};line-height:1.5;"
            f"font-family:{font_family};word-break:break-word;'>"
            f"{html_escape(value_str)}</div>"
            f"</td>"
        )

    def _render_section_heading(self, text: str, count: Optional[int] = None) -> str:
        """Render a section heading with a small uppercase count chip."""
        count_html = ""
        if count is not None:
            count_html = (
                f"<span style='display:inline-block;margin-left:10px;"
                f"padding:2px 8px;background:{_RULE_SOFT};color:{_INK_MUTED};"
                f"border-radius:999px;font-size:11px;font-weight:600;"
                f"letter-spacing:0.04em;font-family:{_FONT_STACK};'>"
                f"{count}</span>"
            )
        return (
            f"<h2 style='font-size:13px;font-weight:600;color:{_INK};"
            f"letter-spacing:0.06em;text-transform:uppercase;"
            f"margin:32px 0 12px 0;font-family:{_FONT_STACK};'>"
            f"{html_escape(text)}{count_html}</h2>"
        )

    def _render_tables_section(
        self,
        heading: str,
        tables: list[dict[str, Any]],
        empty_message: str = "No tables reported.",
    ) -> str:
        """Render a Tables Processed section with zebra rows + dot status."""
        if not tables:
            return (
                self._render_section_heading(heading, 0)
                + f"<p style='color:{_INK_DIM};font-size:13px;margin:8px 0 0 0;"
                f"font-family:{_FONT_STACK};'>{html_escape(empty_message)}</p>"
            )

        # Header row -- uppercase tracked text on a faint surface.
        header = (
            f"<tr>"
            f"<th style='text-align:left;padding:10px 16px;"
            f"border-bottom:1px solid {_RULE};font-size:11px;"
            f"letter-spacing:0.08em;text-transform:uppercase;"
            f"color:{_INK_DIM};font-weight:600;'>Table</th>"
            f"<th style='text-align:left;padding:10px 16px;"
            f"border-bottom:1px solid {_RULE};font-size:11px;"
            f"letter-spacing:0.08em;text-transform:uppercase;"
            f"color:{_INK_DIM};font-weight:600;'>Status</th>"
            f"<th style='text-align:right;padding:10px 16px;"
            f"border-bottom:1px solid {_RULE};font-size:11px;"
            f"letter-spacing:0.08em;text-transform:uppercase;"
            f"color:{_INK_DIM};font-weight:600;'>Rows</th>"
            f"</tr>"
        )

        body = "".join(
            _render_table_row(t, idx)
            for idx, t in enumerate(tables)
        )

        return (
            self._render_section_heading(heading, len(tables))
            + f"""
    <table role='presentation' cellspacing='0' cellpadding='0'
           style='width:100%;border-collapse:collapse;
                  font-family:{_FONT_STACK};'>
      <thead>{header}</thead>
      <tbody>{body}</tbody>
    </table>"""
        )

    def _render_errors_section(self, errors: list[str]) -> str:
        """Render the Errors section as a left-rule, line-numbered callout."""
        if not errors:
            return ""

        # Each line gets a numbered prefix in dim grey.  Hairline bottom
        # rule between lines so long stacktraces remain scannable.
        line_html: list[str] = []
        for idx, line in enumerate(errors, 1):
            text = html_escape(str(line))
            line_html.append(
                f"<tr>"
                f"<td style='padding:6px 10px 6px 0;color:{_INK_DIM};"
                f"font-family:{_MONO_FONT};font-size:11px;"
                f"text-align:right;vertical-align:top;width:1%;"
                f"white-space:nowrap;'>{idx}</td>"
                f"<td style='padding:6px 0;font-family:{_MONO_FONT};"
                f"font-size:12px;color:{_INK};white-space:pre-wrap;"
                f"word-break:break-word;border-bottom:1px solid {_RULE_SOFT};'>"
                f"{text}</td>"
                f"</tr>"
            )

        return (
            self._render_section_heading("Errors", len(errors))
            + f"""
    <div style='border-left:3px solid {_STATUS_RED};padding:4px 0 4px 16px;'>
      <table role='presentation' cellspacing='0' cellpadding='0'
             style='width:100%;border-collapse:collapse;'>
        <tbody>{''.join(line_html)}</tbody>
      </table>
    </div>"""
        )

    def _render_footer(self, batch_info: dict[str, Any]) -> str:
        """Render the slim signed footer."""
        log_file_path = batch_info.get("log_file_path") or "n/a"
        return f"""
    <div style='margin-top:40px;padding-top:20px;
                border-top:1px solid {_RULE};'>
      <div style='font-size:11px;color:{_INK_DIM};
                  font-family:{_FONT_STACK};line-height:1.6;'>
        <div style='margin-bottom:6px;'>
          <span style='letter-spacing:0.06em;text-transform:uppercase;
                       font-weight:600;color:{_INK_MUTED};'>Log</span>
          &nbsp;
          <span style='font-family:{_MONO_FONT};color:{_INK_MUTED};
                       word-break:break-all;'>
            {html_escape(str(log_file_path))}
          </span>
        </div>
        <div style='letter-spacing:0.08em;text-transform:uppercase;
                    font-size:10px;color:{_INK_DIM};font-weight:600;
                    margin-top:12px;'>
          OneFlorida+ &middot; OVID Scheduler &middot; Automated notification
        </div>
      </div>
    </div>"""

    # ─── Top-level render ──────────────────────────────────────────────────

    def _render_html(self, batch_info: dict[str, Any]) -> str:
        """Render the batch_info dict as an HTML email body.

        Args:
            batch_info: Batch metadata dict.

        Returns:
            Complete HTML document as a string.
        """
        hero = self._render_hero(batch_info)
        metadata = self._render_metadata_grid(batch_info)

        cdm_tables = batch_info.get("tables") or []
        cdm_section = self._render_tables_section(
            "Tables processed",
            cdm_tables,
            empty_message="No CDM tables reported.",
        )

        # Raw files section: only render when the scheduler ran the
        # raw_upload stage.  The presence of "raw_upload" in batch_info
        # signals that the stage was attempted (even if it produced no
        # uploaded files); absence means the feature is off entirely
        # for this run.
        raw_section = ""
        raw_upload = batch_info.get("raw_upload")
        if raw_upload and raw_upload.get("executed"):
            raw_tables = batch_info.get("raw_tables") or []
            raw_schema = raw_upload.get("schema") or "RAW_*"
            raw_section = self._render_tables_section(
                f"Raw files uploaded · {raw_schema}",
                raw_tables,
                empty_message="No raw files matched.",
            )

        errors = batch_info.get("errors") or []
        errors_section = self._render_errors_section(errors)
        footer = self._render_footer(batch_info)

        # Outer wrapper: centred 640px reading column on a soft surface.
        return f"""\
<!DOCTYPE html>
<html>
  <head>
    <meta charset='utf-8'>
    <meta name='viewport' content='width=device-width, initial-scale=1.0'>
  </head>
  <body style='margin:0;padding:0;background:{_RULE_SOFT};
               font-family:{_FONT_STACK};color:{_INK};
               -webkit-font-smoothing:antialiased;'>
    <table role='presentation' cellspacing='0' cellpadding='0'
           style='width:100%;border-collapse:collapse;background:{_RULE_SOFT};'>
      <tr>
        <td align='center' style='padding:32px 16px;'>
          <table role='presentation' cellspacing='0' cellpadding='0'
                 style='width:100%;max-width:640px;background:{_SURFACE};
                        border:1px solid {_RULE};border-radius:8px;
                        border-collapse:separate;'>
            <tr>
              <td style='padding:8px 32px 32px 32px;'>
                {hero}
                {metadata}
                {cdm_section}
                {raw_section}
                {errors_section}
                {footer}
              </td>
            </tr>
          </table>
        </td>
      </tr>
    </table>
  </body>
</html>
"""

    def _render_text(self, batch_info: dict[str, Any]) -> str:
        """Render a plain-text fallback body (for clients that prefer text).

        Args:
            batch_info: Batch metadata dict.

        Returns:
            Plain-text summary string.
        """
        lines: list[str] = []
        status = batch_info.get("status", "UNKNOWN")
        batch_id = batch_info.get("batch_id", "unknown_batch")
        duration = _format_duration(batch_info.get("duration"))

        lines.append("OneFlorida+ · OVID Scheduler")
        lines.append("=" * 70)
        lines.append(f"  {batch_id}")
        lines.append(f"  [{status}]   completed in {duration}")
        lines.append("")
        lines.append("-" * 70)
        lines.append("")

        summary = [
            ("Database",    batch_info.get("database")),
            ("DB Server",   batch_info.get("db_server")),
            ("DB Type",     batch_info.get("db_type")),
            ("Upload Type", batch_info.get("upload_type")),
            ("Stages",      batch_info.get("stages")),
            ("Raw Upload",  _format_raw_upload_summary(batch_info)),
            ("Started",     _fmt_dt(batch_info.get("processing_start"))),
            ("Ended",       _fmt_dt(batch_info.get("processing_end"))),
            ("Instance ID", batch_info.get("instance_id")),
        ]
        for label, value in summary:
            display = value if value not in (None, "") else "—"
            lines.append(f"  {label:<14}: {display}")

        # CDM tables.
        lines.append("")
        cdm_tables = batch_info.get("tables") or []
        lines.append(f"Tables processed ({len(cdm_tables)}):")
        if cdm_tables:
            for t in cdm_tables:
                lines.append(_format_text_table_row(t))
        else:
            lines.append("  (none)")

        # Raw upload tables (only when the stage ran).
        raw_upload = batch_info.get("raw_upload")
        if raw_upload and raw_upload.get("executed"):
            raw_tables = batch_info.get("raw_tables") or []
            schema = raw_upload.get("schema") or "RAW_*"
            lines.append("")
            lines.append(f"Raw files uploaded · {schema} ({len(raw_tables)}):")
            if raw_tables:
                for t in raw_tables:
                    lines.append(_format_text_table_row(t))
            else:
                lines.append("  (none)")

        # Errors.
        errors = batch_info.get("errors") or []
        lines.append("")
        lines.append(f"Errors ({len(errors)}):")
        if errors:
            for idx, line in enumerate(errors, 1):
                lines.append(f"  {idx:>3}. {line}")
        else:
            lines.append("  (none)")

        lines.append("")
        lines.append("-" * 70)
        lines.append(f"Log file: {batch_info.get('log_file_path', 'n/a')}")
        lines.append("OneFlorida+ · OVID Scheduler · automated notification")
        return "\n".join(lines)


# ─── Module-level formatting helpers ───────────────────────────────────────

def _render_table_row(table: dict[str, Any], idx: int) -> str:
    """Render a single HTML <tr> for a tables-processed section.

    Uses zebra striping (alternating white / soft-grey) and a coloured
    dot beside the status word.  Counts are right-aligned in monospace.

    Args:
        table: Dict with keys name, status, row_count.
        idx:   Row index (used for zebra striping).

    Returns:
        HTML <tr>...</tr> string.
    """
    name = html_escape(str(table.get("name", "?")))
    status = str(table.get("status", "?"))
    count = table.get("row_count")
    count_str = f"{count:,}" if isinstance(count, int) else "—"
    count_color = _INK if isinstance(count, int) else _INK_DIM

    dot_color = {
        "uploaded":   _STATUS_GREEN,
        "skipped":    _STATUS_AMBER,
        "error":      _STATUS_RED,
        "failed":     _STATUS_RED,
        "not_found":  _INK_DIM,
    }.get(status.lower(), _INK_DIM)

    status_label_color = {
        "uploaded":   _STATUS_GREEN,
        "skipped":    _STATUS_AMBER,
        "error":      _STATUS_RED,
        "failed":     _STATUS_RED,
        "not_found":  _INK_DIM,
    }.get(status.lower(), _INK_MUTED)

    bg = _SURFACE if idx % 2 == 0 else _RULE_SOFT

    dot = (
        f"<span style='display:inline-block;width:8px;height:8px;"
        f"border-radius:50%;background:{dot_color};margin-right:8px;"
        f"vertical-align:middle;'></span>"
    )

    return (
        f"<tr style='background:{bg};'>"
        f"<td style='padding:10px 16px;border-bottom:1px solid {_RULE_SOFT};"
        f"font-family:{_MONO_FONT};font-size:12px;color:{_INK};'>{name}</td>"
        f"<td style='padding:10px 16px;border-bottom:1px solid {_RULE_SOFT};"
        f"font-size:12px;color:{status_label_color};'>"
        f"{dot}{html_escape(status)}</td>"
        f"<td style='padding:10px 16px;border-bottom:1px solid {_RULE_SOFT};"
        f"font-family:{_MONO_FONT};font-size:12px;text-align:right;"
        f"color:{count_color};'>{count_str}</td>"
        f"</tr>"
    )


def _format_text_table_row(t: dict[str, Any]) -> str:
    """Format one table-row entry for the plain-text email body."""
    name = t.get("name", "?")
    status = t.get("status", "?")
    count = t.get("row_count")
    count_str = f"{count:,}" if isinstance(count, int) else "—"
    return f"  {name:<32} {status:<12} {count_str:>15}"


def _format_raw_upload_summary(batch_info: dict[str, Any]) -> str:
    """Render the one-line raw_upload summary for the metadata grid.

    The string is intentionally short so it fits the 2-column grid cell
    without wrapping awkwardly.  Possible outputs:

      * ``"Disabled"``                 -- feature off for this run
      * ``"Skipped (reason)"``         -- attempted but bailed early
      * ``"Succeeded · N files · 12.3s"``
      * ``"Failed · returncode 1"``
      * ``"—"``                        -- never attempted (no key set)

    Args:
        batch_info: Full batch_info dict from the scheduler.

    Returns:
        Human-readable single-line summary.
    """
    raw = batch_info.get("raw_upload")
    if not raw:
        return "—"
    if raw.get("enabled") is False:
        return "Disabled"
    if not raw.get("executed"):
        reason = raw.get("error_summary") or "skipped"
        return f"Skipped · {reason}"
    if raw.get("success"):
        # Prefer explicit file_count when scheduler supplies it; fall back
        # to len(raw_tables) which the email renderer already has.
        file_count = raw.get("file_count")
        if file_count is None:
            file_count = len(batch_info.get("raw_tables") or [])
        duration = _format_duration(raw.get("duration"))
        return f"Succeeded · {file_count} files · {duration}"
    rc = raw.get("returncode")
    rc_str = f"returncode {rc}" if rc is not None else "see errors"
    return f"Failed · {rc_str}"


def _fmt_dt(value: Optional[Any]) -> str:
    """Format a datetime (or string) for display in the email.

    Args:
        value: A datetime object, ISO string, or None.

    Returns:
        Human-readable datetime string, or 'n/a' if value is None.
    """
    if value is None:
        return "n/a"
    if isinstance(value, datetime.datetime):
        return value.strftime("%Y-%m-%d %H:%M:%S")
    return str(value)


def _format_duration(delta: Optional[Any]) -> str:
    """Format a timedelta as a human-readable duration string.

    Args:
        delta: A datetime.timedelta, a numeric seconds value, or None.

    Returns:
        String like '2h 14m 37s' or 'n/a' if delta is None.
    """
    if delta is None:
        return "n/a"

    if isinstance(delta, datetime.timedelta):
        total_seconds = int(delta.total_seconds())
    else:
        try:
            total_seconds = int(delta)
        except (TypeError, ValueError):
            return "n/a"

    if total_seconds < 0:
        total_seconds = 0

    hours, remainder = divmod(total_seconds, 3600)
    minutes, seconds = divmod(remainder, 60)

    if hours:
        return f"{hours}h {minutes}m {seconds}s"
    if minutes:
        return f"{minutes}m {seconds}s"
    return f"{seconds}s"
