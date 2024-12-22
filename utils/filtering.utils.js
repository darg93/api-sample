const DISALLOWED_VALUES = [
  "[not provided]",
  "placeholder",
  "[[unknown]]",
  "not set",
  "not provided",
  "unknown",
  "undefined",
  "n/a",
];

const filterNullValuesFromObject = (object) =>
  Object.fromEntries(
    Object.entries(object).filter(
      ([_, v]) =>
        v !== null &&
        v !== "" &&
        typeof v !== "undefined" &&
        (typeof v !== "string" ||
          !DISALLOWED_VALUES.includes(v.toLowerCase()) ||
          !v.toLowerCase().includes("!$record"))
    )
  );

const normalizePropertyName = (key) =>
  key
    .toLowerCase()
    .replace(/__c$/, "")
    .replace(/^_+|_+$/g, "")
    .replace(/_+/g, "_");

const generateLastModifiedDateFilter = (
  date,
  nowDate,
  propertyName = "hs_lastmodifieddate"
) => ({
  filters: date
    ? [
        {
          propertyName,
          operator: "GTE",
          value: `${date.valueOf()}`,
        },
        {
          propertyName,
          operator: "LTE",
          value: `${nowDate.valueOf()}`,
        },
      ]
    : [],
});

module.exports = {
  filterNullValuesFromObject,
  normalizePropertyName,
  generateLastModifiedDateFilter,
};
