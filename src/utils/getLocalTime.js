export default function getLocalTime () {
  return `[${new Date().toLocaleString('en-US', { timeZone: 'America/Sao_Paulo' }).replace(/(.*, )/g, "")}]`;
};
