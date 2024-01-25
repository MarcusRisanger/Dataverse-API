from dataverse_api.metadata.complex_properties import Label, create_label


def define_label(label: str | Label | None = None, override: str = "", *, language_code: int | None = None) -> Label:
    """
    For handling standard label input allowing the submission of
    different label types and different labels created depending on input.

    If no input is given, it will return an "empty" `Label` with
    default language code ID.

    Parameters
    ----------
    label : str or Label
        If a string is passed, the function will interpret this as the
        desired actual label value, using the default language code ID.
        If a `Label` class is passed, it will be returned unprocessed.
    override : str
        Optional argument if no `label` is given. Defaults to an empty string.

    Returns
    -------
    Label
        The defined label metadata according to the specifications.
    """
    if isinstance(label, Label):
        return label
    if isinstance(label, str) and language_code is None:
        return create_label(label=label)
    if isinstance(label, str) and isinstance(language_code, int):
        return create_label(label=label, language_code=language_code)
    if label is None:
        return create_label(label=override)

    raise TypeError("Wrong type supplied!")
