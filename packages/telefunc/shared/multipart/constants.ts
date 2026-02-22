export const FORM_DATA_MAIN_FIELD = '__telefunc_main'
export const SERIALIZER_PLACEHOLDER_KEY = '__telefunc_multipart'
export const SERIALIZER_PREFIX_FILE = '!TelefuncFile:'
export const SERIALIZER_PREFIX_BLOB = '!TelefuncBlob:'

export type FileMetadata = { key: string; name: string; size: number; type: string; lastModified: number }
export type BlobMetadata = { key: string; size: number; type: string }
