'use strict';
const crypto = require('crypto');
const C = require('./constants.json');

function randInt(n) { return crypto.randomInt(0, n); }

function genCode() {
  let s = '';
  for (let i = 0; i < C.code_length; i++) s += C.code_alphabet[randInt(C.code_alphabet.length)];
  return s;
}
function genIcons() {
  const out = [];
  for (let i = 0; i < C.icon_code_length; i++) out.push(randInt(C.icons.length));
  return out;
}
// Accept lowercase / Turkish keyboard letters / spaces. The in-game keypad only shows the alphabet anyway.
function normalizeCode(code) {
  if (typeof code !== 'string') return null;
  let s = code.toLocaleUpperCase('tr-TR').replace(/[\s-]/g, '');
  s = s.replace(/İ/g, 'I').replace(/Ş/g, 'S').replace(/Ç/g, 'C').replace(/Ğ/g, 'G').replace(/Ü/g, 'U');
  if (s.length !== C.code_length) return null;
  for (const ch of s) if (!C.code_alphabet.includes(ch)) return null;
  return s;
}
function iconsKey(icons) {
  if (!Array.isArray(icons) || icons.length !== C.icon_code_length) return null;
  for (const i of icons) if (!Number.isInteger(i) || i < 0 || i >= C.icons.length) return null;
  return icons.join('-');
}
module.exports = { genCode, genIcons, normalizeCode, iconsKey };
