"""subagent:notifier — envoie l'alerte formatée (email et/ou Telegram).

- Format : modèle + réf + année + set, prix demandé, prix de référence, décote %,
  localisation, lien direct, 1-2 photos, et la phrase "pourquoi c'est une affaire".
- Regroupe plusieurs deals d'une même fenêtre en un seul digest.
- Canaux configurés via NOTIFY_CHANNELS (.env) ; secrets jamais codés en dur.
"""
from __future__ import annotations

import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

import httpx

from ..config import get_settings
from ..logging_conf import get_logger
from ..models import Deal

log = get_logger("agent.notifier")


# --------------------------------------------------------------------- format
def format_deal_text(deal: Deal) -> str:
    ls = deal.listing
    year = ls.year or "?"
    lines = [
        f"• {ls.brand} {ls.model} {ls.reference} ({year}) — {ls.set.value}",
        f"  Prix demandé : ${ls.asking_price_usd:,.0f}  |  Référence marché : ${deal.reference_price_usd:,.0f}",
        f"  Décote : {deal.discount_pct}%  ({deal.comparables_count} comparables)",
        f"  Localisation : {ls.location or 'n/a'}  |  Vendeur : {ls.seller or 'n/a'}",
        f"  Pourquoi : {deal.reason}",
        f"  Lien : {ls.url}",
    ]
    if ls.images:
        lines.append("  Photos : " + " ".join(ls.images[:2]))
    return "\n".join(lines)


def format_deal_html(deal: Deal) -> str:
    ls = deal.listing
    year = ls.year or "?"
    imgs = "".join(
        f'<img src="{u}" style="max-width:240px;margin:4px;border-radius:6px">'
        for u in ls.images[:2]
    )
    badge = "#c0392b" if deal.is_suspicious else "#27ae60"
    return f"""
    <div style="border:1px solid #ddd;border-radius:8px;padding:12px;margin:10px 0;font-family:Arial">
      <h3 style="margin:0 0 6px">{ls.brand} {ls.model} {ls.reference}
        <span style="background:{badge};color:#fff;padding:2px 8px;border-radius:12px;font-size:13px">
          -{deal.discount_pct}%</span></h3>
      <p style="margin:2px 0">Année {year} · {ls.set.value} · {ls.location or 'n/a'}</p>
      <p style="margin:2px 0"><b>${ls.asking_price_usd:,.0f}</b>
        <span style="color:#888">(réf marché ${deal.reference_price_usd:,.0f},
        {deal.comparables_count} comparables)</span></p>
      <p style="margin:6px 0;color:#333">{deal.reason}</p>
      <div>{imgs}</div>
      <p><a href="{ls.url}">Voir l'annonce →</a></p>
    </div>"""


def build_digest(deals: list[Deal]) -> tuple[str, str, str]:
    """Construit (sujet, corps_texte, corps_html) pour un lot de deals."""
    n = len(deals)
    subject = f"[Deal Hunter] {n} bonne(s) affaire(s) détectée(s)"
    if any(d.is_suspicious for d in deals):
        subject += " ⚠️"
    text = f"{n} affaire(s) détectée(s) :\n\n" + "\n\n".join(
        format_deal_text(d) for d in deals
    )
    html = (
        f"<h2>{n} bonne(s) affaire(s) détectée(s)</h2>"
        + "".join(format_deal_html(d) for d in deals)
    )
    return subject, text, html


# --------------------------------------------------------------------- canaux
class NotifierAgent:
    def __init__(self):
        self.settings = get_settings()

    def notify(self, deals: list[Deal]) -> bool:
        """Envoie un digest sur tous les canaux configurés. True si au moins un OK."""
        if not deals:
            return False
        subject, text, html = build_digest(deals)
        sent = False
        for channel in self.settings.notify_channels:
            try:
                if channel == "email":
                    self._send_email(subject, text, html)
                    sent = True
                elif channel == "telegram":
                    self._send_telegram(text)
                    sent = True
                else:
                    log.warning("Canal de notification inconnu: %s", channel)
            except Exception as exc:  # on n'interrompt pas les autres canaux
                log.error("Échec notification %s: %s", channel, exc)
        return sent

    def _send_email(self, subject: str, text: str, html: str) -> None:
        s = self.settings
        if not (s.smtp_host and s.smtp_from and s.smtp_to):
            raise RuntimeError("Config SMTP incomplète (SMTP_HOST/FROM/TO)")
        msg = MIMEMultipart("alternative")
        msg["Subject"] = subject
        msg["From"] = s.smtp_from
        msg["To"] = ", ".join(s.smtp_to)
        msg.attach(MIMEText(text, "plain", "utf-8"))
        msg.attach(MIMEText(html, "html", "utf-8"))
        with smtplib.SMTP(s.smtp_host, s.smtp_port, timeout=30) as server:
            if s.smtp_use_tls:
                server.starttls()
            if s.smtp_user:
                server.login(s.smtp_user, s.smtp_password)
            server.sendmail(s.smtp_from, s.smtp_to, msg.as_string())
        log.info("Email envoyé à %s", ", ".join(s.smtp_to))

    def _send_telegram(self, text: str) -> None:
        s = self.settings
        if not (s.telegram_bot_token and s.telegram_chat_id):
            raise RuntimeError("Config Telegram incomplète (TOKEN/CHAT_ID)")
        url = f"https://api.telegram.org/bot{s.telegram_bot_token}/sendMessage"
        resp = httpx.post(
            url,
            json={"chat_id": s.telegram_chat_id, "text": text, "disable_web_page_preview": False},
            timeout=20.0,
        )
        resp.raise_for_status()
        log.info("Message Telegram envoyé")
