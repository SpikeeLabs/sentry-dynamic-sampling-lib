const scope = [
  "admin",
  "command",
  "model",
  "route",
  "task",
  "validator",
  "sampler"
];

module.exports = {
  extends: ["@commitlint/config-angular"],
  rules: {
    "body-empty": [2, "never", false],
    "scope-enum": [2, "always", scope],
  },
};
