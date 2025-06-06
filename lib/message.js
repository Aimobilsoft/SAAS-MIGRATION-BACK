class MessageBuilder {
    constructor() {
      this.success = false;
      this.origin = "";
      this.process = "0";
      this.status = undefined;
      this.message = "Failed transaction";
      this.documents = undefined;
    }
    setSuccess(success) {
      this.success = success;
      return this;
    }
    setOrigen(origin) {
      this.origin = origin;
      return this;
    }
    setProcess(process) {
      this.process = process;
      return this;
    }
    setStatus(status) {
      this.status = status;
      return this;
    }
    setMessage(message) {
      this.message = message;
      return this;
    }
    setDocuments(documents) {
      this.documents = documents;
      return this;
    }
    build() {
      return {
        success: this.success,
        origin: this.origin,
        process: this.process,
        status: this.status,
        message:
          this.message ||
          (this.success ? "Execution successfully" : "Failed transaction"),
        documents: this.documents
      };
    }
  }
  
  module.exports = { MessageBuilder };
  