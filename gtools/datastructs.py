from collections import namedtuple

ModelInfo = namedtuple("ModelInfo", [
    "wandb_id", "lesion_start_epoch", "lesion_type", "model_type", "run_name", 
    "train_data", "test_data", "mask_value", "lstm_units", "learning_rate",
    "batch_size", "frequency_scale_k", "epochs", "seed", "orth_features",
    "phon_features", "phon_max_length", "name", "bucket_name"])


BaseModelState = namedtuple("ModelState", [
    "encoder_cell_state", "encoder_hidden_state", "decoder_cell_state",
    "decoder_hidden_state", "output", "name", "bucket_name"])


class ModelState(BaseModelState):
    __slots__ = ()
    def nitems(self):
        return self.encoder_cell.shape[0]

    def nunits(self):
        return self.encoder_cell.shape[-1]

    def phon_max_length(self):
        return self.output.shape[1]

