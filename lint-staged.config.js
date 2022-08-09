export default {
  '**/*.{json,yml}': ['prettier --write'],
  'packages/*/*.js': ['prettier --write', 'standard --fix']
}
