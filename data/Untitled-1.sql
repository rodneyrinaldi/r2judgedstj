SELECT *
FROM public.judged
LIMIT 100;


ALTER TABLE public.judged
RENAME COLUMN classepadronizada TO "classePadronizada";


ALTER TABLE judged
DROP COLUMN numerodocumento;


ALTER TABLE public.judged
ADD COLUMN "numerodocumento" VARCHAR(100) NULL;
