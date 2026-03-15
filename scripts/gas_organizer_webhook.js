/**
 * Google Apps Script for "Органайзер" sheet — Search Integration
 *
 * When a user types a search query into a designated cell,
 * sends it to the bot via webhook. The bot searches its DB
 * and writes the result back to the adjacent cell(s).
 *
 * INSTALLATION:
 * 1. Open the Google Sheet containing "Органайзер"
 * 2. Extensions → Apps Script
 * 3. Paste this code
 * 4. Save, then: Triggers → Add trigger → onEditOrganizer → From spreadsheet → On edit
 *
 * USAGE:
 * - Type an invoice number or search query into the "Поиск бот" column
 * - The bot will find the invoice and populate adjacent cells with result info
 * - Clear the search cell to clear results
 */

// ============ CONFIG ============
var ORG_CONFIG = {
  BOT_WEBHOOK_URL: "http://46.23.98.118:8443/webhooks/sheets",
  WEBHOOK_SECRET: "77",
  SHEET_NAME: "Органайзер",

  // Search column (0-based): where user types query
  // Row 3 in Органайзер = invoice numbers. This is for a SEPARATE search area.
  // Using row 9+ for search interface (below the data area):
  SEARCH_ROW: 9,          // Row 9 — search query row
  RESULT_ROW: 10,         // Row 10 — result row (bot writes here)

  // Or: use a dedicated column approach (set to -1 to disable row-based search)
  // For column-based search in _Органайзер (бот):
  BOT_SHEET_NAME: "_Органайзер (бот)",
  SEARCH_COL: 0,          // A column (0-based) — search query
  RESULT_COL: 2,          // C column — where bot writes result
  HEADER_ROW: 1,          // Skip header
};
// ================================


function onEditOrganizer(e) {
  if (!e || !e.range) return;

  var sheet = e.source.getActiveSheet();
  var sheetName = sheet.getName();

  // --- Option 1: Search in Органайзер (horizontal) row 9 ---
  if (sheetName === ORG_CONFIG.SHEET_NAME) {
    var row = e.range.getRow();
    if (row !== ORG_CONFIG.SEARCH_ROW) return;

    var col = e.range.getColumn();  // 1-based
    var query = String(e.range.getValue()).trim();
    if (!query) return;

    sendOrgWebhook({
      type: "search",
      query: query,
      sheet: ORG_CONFIG.SHEET_NAME,
      result_cell: "R" + ORG_CONFIG.RESULT_ROW + "C" + col,  // R10C{col} notation
      result_row: ORG_CONFIG.RESULT_ROW,
      result_col: col,
      source: "organizer"
    });
    return;
  }

  // --- Option 2: Search in _Органайзер (бот) (vertical) column A ---
  if (sheetName === ORG_CONFIG.BOT_SHEET_NAME) {
    var row = e.range.getRow();
    if (row <= ORG_CONFIG.HEADER_ROW) return;

    var col = e.range.getColumn() - 1;  // 0-based
    if (col !== ORG_CONFIG.SEARCH_COL) return;

    var query = String(e.range.getValue()).trim();
    if (!query) return;

    sendOrgWebhook({
      type: "search",
      query: query,
      sheet: ORG_CONFIG.BOT_SHEET_NAME,
      result_row: row,
      result_col: ORG_CONFIG.RESULT_COL + 1,  // 1-based for Sheets API
      source: "organizer_bot"
    });
  }
}


function sendOrgWebhook(payload) {
  var options = {
    method: "post",
    contentType: "application/json",
    headers: {
      "X-Webhook-Secret": ORG_CONFIG.WEBHOOK_SECRET
    },
    payload: JSON.stringify(payload),
    muteHttpExceptions: true,
    connectTimeout: 10000,
    timeout: 15000
  };

  try {
    var response = UrlFetchApp.fetch(ORG_CONFIG.BOT_WEBHOOK_URL, options);
    var code = response.getResponseCode();
    if (code !== 200) {
      Logger.log("Organizer webhook error: " + code + " — " + response.getContentText());
    }
  } catch (err) {
    Logger.log("Organizer webhook failed: " + err.message);
  }
}
