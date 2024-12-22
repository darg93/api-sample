class Logger {
  constructor() {
    this.env = process.env.NODE_ENV || 'development';
  }

  formatMessage(level, message, meta = {}) {
    const timestamp = new Date().toISOString();
    return {
      timestamp,
      level,
      message,
      ...meta,
      env: this.env
    };
  }

  log(level, message, meta) {
    const formattedMessage = this.formatMessage(level, message, meta);
    console.log(JSON.stringify(formattedMessage));
  }

  info(message, meta) {
    this.log('info', message, meta);
  }

  error(message, meta) {
    if (meta?.error) {
      meta.errorMessage = meta.error.message;
      meta.stack = meta.error.stack;
      delete meta.error;
    }
    this.log('error', message, meta);
  }

  warn(message, meta) {
    this.log('warn', message, meta);
  }

  debug(message, meta) {
    if (this.env === 'development') {
      this.log('debug', message, meta);
    }
  }
}

module.exports = new Logger();