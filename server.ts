import express from "express";
import { createServer as createViteServer } from "vite";
import path from "path";
import fs from "fs";
import Papa from "papaparse";
import iconv from "iconv-lite";

import multer from "multer";

async function startServer() {
  interface AuditTrail {
  trail: string;
  id?: string;
  value?: any;
  detail: string;
  resolution?: string;
  fullData?: string;
  cod_orgao?: string;
  seq_orgao?: string;
  nom_orgao?: string;
}

const app = express();
  const PORT = 3000;

  app.use(express.json({ limit: '50mb' }));

  const folders = [
    "Frota", "Saúde", "Receita", "Licitação", "Empenho", 
    "Desp_Pessoal", "Despesa", "Educação", "Contrato", "Órgão"
  ];

  // Map display names to filesystem-safe names to avoid accent issues
  const folderMapping: Record<string, string> = {
    "Frota": "Frota",
    "Saúde": "Saude",
    "Receita": "Receita",
    "Licitação": "Licitacao",
    "Empenho": "Empenho",
    "Desp_Pessoal": "Desp_Pessoal",
    "Despesa": "Despesa",
    "Educação": "Educacao",
    "Contrato": "Contrato",
    "Órgão": "Orgao"
  };

  const baseDataDir = path.join(process.cwd(), "data");
  const uploadDir = path.join(baseDataDir, "uploads");
  if (!fs.existsSync(baseDataDir)) fs.mkdirSync(baseDataDir);
  if (!fs.existsSync(uploadDir)) fs.mkdirSync(uploadDir);
  
  folders.forEach(f => {
    const fsFolder = folderMapping[f] || f;
    const dir = path.join(baseDataDir, fsFolder);
    if (!fs.existsSync(dir)) fs.mkdirSync(dir);
  });

  // Configure multer for file uploads to a temporary directory
  const storage = multer.diskStorage({
    destination: (req, file, cb) => {
      cb(null, uploadDir);
    },
    filename: (req, file, cb) => {
      // Sanitize filename to avoid issues with special characters
      const sanitized = file.originalname.replace(/[^a-zA-Z0-9._-]/g, '_');
      const uniqueSuffix = Date.now() + '-' + Math.round(Math.random() * 1E9);
      cb(null, uniqueSuffix + '-' + sanitized);
    }
  });
  const upload = multer({ 
    storage,
    limits: { fileSize: 100 * 1024 * 1024 } // 100MB limit
  });

  app.post("/api/upload", (req: any, res) => {
    upload.single("file")(req, res, (err) => {
      if (err) {
        console.error("Upload Error Detail:", err);
        if (err instanceof multer.MulterError) {
          return res.status(400).json({ error: `Erro do Multer: ${err.message} (${err.code})` });
        }
        return res.status(500).json({ error: `Erro no servidor durante upload: ${err.message}` });
      }
      
      if (!req.file) {
        return res.status(400).json({ error: "Nenhum arquivo foi recebido pelo servidor." });
      }
      
      const displayFolder = req.body.folder || "Órgão";
      const fsFolder = folderMapping[displayFolder] || displayFolder;
      const targetDir = path.join(baseDataDir, fsFolder);
      
      if (!fs.existsSync(targetDir)) {
        try {
          fs.mkdirSync(targetDir, { recursive: true });
        } catch (mkdirErr) {
          console.error(`Error creating directory ${targetDir}:`, mkdirErr);
          return res.status(500).json({ error: `Erro ao criar diretório de destino: ${displayFolder}` });
        }
      }

      // Sanitize filename for final destination
      const sanitizedFilename = req.file.originalname.replace(/[^a-zA-Z0-9._-]/g, '_');
      const finalPath = path.join(targetDir, sanitizedFilename);
      
      try {
        // Move file from temp upload dir to final folder
        // Use copy + unlink instead of rename to handle cross-device issues
        fs.copyFileSync(req.file.path, finalPath);
        fs.unlinkSync(req.file.path);
        console.log(`Arquivo ${sanitizedFilename} movido com sucesso para ${fsFolder} (${displayFolder})`);
        
        res.json({ 
          success: true, 
          filename: sanitizedFilename, 
          folder: displayFolder,
          size: req.file.size
        });
      } catch (moveErr) {
        console.error(`Error moving file to ${finalPath}:`, moveErr);
        // Cleanup temp file if move fails
        if (fs.existsSync(req.file.path)) {
          fs.unlinkSync(req.file.path);
        }
        res.status(500).json({ error: `Erro ao salvar arquivo na pasta final: ${displayFolder}` });
      }
    });
  });

  const parseNumber = (val: any): number => {
    if (val === null || val === undefined) return 0;
    if (typeof val === 'number') return val;
    if (typeof val === 'string') {
      let sanitized = val.trim();
      // Handle Brazilian format (1.234,56) vs standard (1234.56)
      if (sanitized.includes(',') && sanitized.includes('.')) {
        sanitized = sanitized.replace(/\./g, '').replace(',', '.');
      } else if (sanitized.includes(',')) {
        sanitized = sanitized.replace(',', '.');
      }
      const parsed = parseFloat(sanitized);
      return isNaN(parsed) ? 0 : parsed;
    }
    return 0;
  };

  const parseDate = (val: any): Date | null => {
    if (!val) return null;
    if (val instanceof Date) return val;
    if (typeof val === 'string') {
      const parts = val.split('/');
      if (parts.length === 3) {
        const d = parseInt(parts[0], 10);
        const m = parseInt(parts[1], 10) - 1;
        const y = parseInt(parts[2], 10);
        const date = new Date(y, m, d);
        return isNaN(date.getTime()) ? null : date;
      }
      const date = new Date(val);
      return isNaN(date.getTime()) ? null : date;
    }
    return null;
  };

  const sanitizeId = (id: any): string => {
    if (id === null || id === undefined) return "";
    return String(id).trim().replace(/\D/g, '');
  };

  const compareIds = (id1: any, id2: any): boolean => {
    const s1 = String(id1 || "").trim();
    const s2 = String(id2 || "").trim();
    if (!s1 || !s2) return false;
    return s1 === s2 || sanitizeId(s1) === sanitizeId(s2);
  };

  const readCSV = (folder: string, filenamePattern?: string): any[] => {
    // Map display folder to filesystem folder
    const fsFolder = folderMapping[folder] || folder;
    
    // Search in the specific folder and also in variations
    const searchFolders = [fsFolder];
    
    // Add variations for common folder names (both display and fs names)
    const variations: Record<string, string[]> = {
      "Empenho": ["Despesa", "Despesas", "Empenho", "Pagamento", "Liquidacao", "Notas_Fiscais", "Desp_Pessoal"],
      "Despesa": ["Despesa", "Despesas", "Empenho", "Pagamento", "Liquidacao", "Notas_Fiscais", "Desp_Pessoal"],
      "Licitação": ["Licitacao", "Licitações", "Licitacao_Hab", "Dispensa", "Contrato"],
      "Licitacao": ["Licitação", "Licitações", "Licitacao_Hab", "Dispensa", "Contrato"]
    };

    if (variations[folder]) {
      searchFolders.push(...variations[folder].map(v => folderMapping[v] || v));
    }

    const uniqueSearchFolders = Array.from(new Set(searchFolders));
    
    let allData: any[] = [];
    
    const tryFolders = (folderList: string[]) => {
      let data: any[] = [];
      folderList.forEach(f => {
        const dir = path.join(baseDataDir, f);
        if (!fs.existsSync(dir)) return;
        
        let files: string[] = [];
        try {
          files = fs.readdirSync(dir).filter(file => {
            if (!file.toLowerCase().endsWith(".csv")) return false;
            if (!filenamePattern) return true;
            
            const normalizedFile = file.toLowerCase().replace(/_/g, '');
            const normalizedPattern = filenamePattern.toLowerCase().replace(/_/g, '');
            return normalizedFile.includes(normalizedPattern);
          });
        } catch (err) {
          console.error(`Error reading directory ${dir}:`, err);
          return;
        }
        
        files.forEach(file => {
          try {
            const filePath = path.join(dir, file);
            const buffer = fs.readFileSync(filePath);
            
            if (buffer.length === 0) return;

            // Try to detect encoding more robustly
            let content = "";
            try {
              content = buffer.toString("utf8");
              if (content.includes("\ufffd")) {
                content = iconv.decode(buffer, "iso-8859-1");
              }
            } catch (e) {
              content = iconv.decode(buffer, "iso-8859-1");
            }
            
            const parsed = Papa.parse(content, {
              header: true,
              delimiter: "", // Auto-detect delimiter
              dynamicTyping: true,
              skipEmptyLines: true,
              transform: (value) => {
                if (value === "-9" || value === -9 || value === "" || value === undefined || value === null) return null;
                if (typeof value === 'string') return value.trim();
                return value;
              }
            });
            
            if (parsed.errors && parsed.errors.length > 0) {
              console.warn(`CSV Parsing warnings for ${file}:`, parsed.errors.slice(0, 3));
            }

            if (parsed.data && Array.isArray(parsed.data)) {
              data = [...data, ...parsed.data];
            }
          } catch (err) {
            console.error(`Error parsing file ${file} in ${f}:`, err);
          }
        });
      });
      return data;
    };

    allData = tryFolders(uniqueSearchFolders);
    
    // Fallback to all folders if no data found with pattern
    if (allData.length === 0 && filenamePattern) {
      const allFsFolders = Object.values(folderMapping);
      const remainingFolders = allFsFolders.filter(f => !uniqueSearchFolders.includes(f));
      allData = tryFolders(remainingFolders);
    }
    
    return allData;
  };

  app.get("/api/files", (req, res) => {
    const status = folders.map(folder => {
      const fsFolder = folderMapping[folder] || folder;
      const dir = path.join(baseDataDir, fsFolder);
      const files = fs.existsSync(dir) ? fs.readdirSync(dir).filter(f => f.toLowerCase().endsWith(".csv")) : [];
      return { folder, count: files.length, files };
    });
    res.json(status);
  });

  app.post("/api/files/clear", (req, res) => {
    try {
      folders.forEach(folder => {
        const fsFolder = folderMapping[folder] || folder;
        const dir = path.join(baseDataDir, fsFolder);
        if (fs.existsSync(dir)) {
          const files = fs.readdirSync(dir);
          files.forEach(file => {
            if (file.toLowerCase().endsWith(".csv")) {
              try {
                fs.unlinkSync(path.join(dir, file));
              } catch (e) {
                console.error(`Error deleting file ${file} in ${fsFolder}:`, e);
              }
            }
          });
        }
      });
      res.json({ success: true, message: "Todas as tabelas foram limpas." });
    } catch (error) {
      console.error("Erro ao limpar tabelas:", error);
      res.status(500).json({ error: "Erro ao limpar tabelas." });
    }
  });

  app.get("/api/entities", (req, res) => {
    try {
      const orgaoData = readCSV("Órgão", "ORGAO");
      const entities = orgaoData
        .filter(o => o.nom_orgao || o.dsc_unidade || o.nom_unidade_gestora)
        .map(o => ({
          cod_orgao: String(o.cod_orgao || "").trim(),
          nom_orgao: String(o.nom_orgao || o.dsc_unidade || o.nom_unidade_gestora || "").trim()
        }))
        .filter((v, i, a) => v.cod_orgao && v.nom_orgao && a.findIndex(t => t.cod_orgao === v.cod_orgao) === i);
      res.json(entities);
    } catch (error) {
      res.status(500).json({ error: "Erro ao carregar entidades." });
    }
  });

  const fieldDictionary: Record<string, string> = {
    "num_empenho": "Número do Empenho",
    "dsc_objeto": "Descrição do Objeto",
    "cod_orgao": "Código do Órgão",
    "nom_orgao": "Nome do Órgão",
    "num_cpf": "CPF do Servidor",
    "nom_servidor": "Nome do Servidor",
    "vlr_empenho": "Valor do Empenho",
    "dat_empenho": "Data do Empenho",
    "seq_veiculo": "Sequencial do Veículo",
    "num_placa": "Placa do Veículo",
    "num_marc_inicial": "Quilometragem Inicial",
    "num_marc_final": "Quilometragem Final",
    "num_quant_utilizada": "Quantidade Utilizada",
    "dsc_tipo_gasto": "Tipo de Gasto",
    "num_licitacao": "Número da Licitação",
    "num_contrato": "Número do Contrato",
    "num_processo": "Número do Processo",
    "num_documento_credor": "Documento do Credor (CPF/CNPJ)",
    "nom_credor": "Nome do Credor",
    "dat_vencimento": "Data de Vencimento",
    "vlr_liquido": "Valor Líquido",
    "vlr_bruto": "Valor Bruto",
    "vlr_desconto": "Valor de Desconto",
    "dsc_cargo": "Cargo do Servidor",
    "num_mes_referencia": "Mês de Referência",
    "num_ano_referencia": "Ano de Referência",
    "dsc_unidade": "Unidade Gestora",
    "nom_unidade_gestora": "Nome da Unidade Gestora",
    "seq_orgao": "Sequencial do Órgão",
    "cod_unidade_gestora": "Código da Unidade Gestora",
    "cod_unidade": "Código da Unidade",
    "nom_unidade": "Nome da Unidade",
    "dsc_funcao": "Função",
    "dsc_subfuncao": "Subfunção",
    "dsc_programa": "Programa",
    "dsc_acao": "Ação",
    "dsc_elemento_despesa": "Elemento de Despesa",
    "vlr_pago": "Valor Pago",
    "dat_pagamento": "Data de Pagamento",
    "num_nota_fiscal": "Número da Nota Fiscal",
    "num_serie_nota_fiscal": "Série da Nota Fiscal",
    "dat_emissao_nota_fiscal": "Data de Emissão da Nota Fiscal",
    "num_chassi": "Chassi do Veículo",
    "num_renavam": "Renavam do Veículo",
    "dsc_marca": "Marca do Veículo",
    "dsc_modelo": "Modelo do Veículo",
    "num_ano_fabricacao": "Ano de Fabricação",
    "num_ano_modelo": "Ano do Modelo",
    "dsc_combustivel": "Tipo de Combustível",
    "vlr_unitario": "Valor Unitário",
    "num_quant": "Quantidade",
    "dsc_unidade_medida": "Unidade de Medida",
    "vlr_total": "Valor Total",
    "num_documento": "Número do Documento",
    "nom_socio": "Nome do Sócio",
    "num_documento_resp": "Documento do Responsável",
    "nom_resp": "Nome do Responsável",
    "dat_inicio_vigencia": "Data de Início da Vigência",
    "dat_fim_vigencia": "Data de Fim da Vigência",
    "vlr_contrato": "Valor do Contrato",
    "dsc_objeto_contrato": "Objeto do Contrato",
    "num_ano_licitacao": "Ano da Licitação",
    "cod_modalidade_licitacao": "Modalidade da Licitação",
    "num_processo_licitatorio": "Processo Licitatório",
    "dat_abertura": "Data de Abertura",
    "vlr_estimado": "Valor Estimado",
    "vlr_homologado": "Valor Homologado",
    "nom_vencedor": "Nome do Vencedor",
    "num_cpf_cnpj_vencedor": "CPF/CNPJ do Vencedor"
  };

  app.post("/api/audit/process", async (req, res) => {
    try {
      const results: AuditTrail[] = [];
      
      // General data loads for reuse
      const orgaoGeral = readCSV("Órgão", "ORGAO");
      const empenhosGeral = readCSV("Despesa", "EMPENHO");
      const receitasGeral = readCSV("Receita", "RECEITA");
      const licitacoesGeral = readCSV("Licitação", "LICITACAO");
      const itensLicitGeral = readCSV("Licitação", "ITEMLICITACAO");
      const pessoalGeral = readCSV("Desp_Pessoal", "PESSOAL");
      const gastoFrotaGeral = readCSV("Frota", "GASTOFROTA");
      const frotaGeral = readCSV("Frota", "FROTA");
      const manutencaoFrotaGeral = readCSV("Frota", "MANUTENCAOFROTA");
      const liquidacoesGeral = readCSV("Despesa", "LIQUIDACAO");
      const contratosGeral = readCSV("Contrato", "CONTRATO");
      const termosContratoGeral = readCSV("Contrato", "TERMOCONTRATO");
      const qsaGeral = readCSV("Licitação", "QUADROSOCLICITACAO");
      const itensDispensaGeral = readCSV("Dispensa", "ITEMDISPENSA");
      const habLicitacaoGeral = readCSV("Licitação", "HABLICITACAO");
      const pagamentosGeral = readCSV("Pagamento", "PAGAMENTO");
      const pagamentosDespesaGeral = readCSV("Despesa", "PAGAMENTO");
      const orgaoRespGeral = readCSV("Órgão", "ORGAORESP");
      const despPessoalGeral = readCSV("Desp_Pessoal", "DESPPESSOAL");
      const regAdesaoGeral = readCSV("Licitação", "REGADESAO");
      const notasFiscaisGeral = readCSV("Despesa", "NOTASFISCAIS");
      const itemNfsGeral = readCSV("Despesa", "ITEMNFS");
      const movPagamentoGeral = readCSV("Pagamento", "MOVPAGAMENTO");
      const dispensasGeral = readCSV("Licitação", "DISPENSA");
      const itensEmpenhoGeral = readCSV("Despesa", "ITEMEMPENHO");
      const homologacoesGeral = readCSV("Licitação", "HOMOLICITACAO");

      const orgaoData = orgaoGeral;
      const orgaoMap = orgaoGeral.reduce((acc: any, o: any) => {
        const cod = String(o.cod_orgao || "").trim();
        const seq = String(o.seq_orgao || "").trim();
        const nom = String(o.nom_orgao || o.dsc_unidade || o.nom_unidade_gestora || "").trim();
        const isMain = !!o.nom_orgao;
        
        if (nom && nom !== "null" && nom !== "undefined") {
          const update = (key: string) => {
            if (!key || key === "null" || key === "undefined") return;
            const existingIsMain = acc[key + "_is_main"];
            if (!acc[key] || (isMain && !existingIsMain)) {
              acc[key] = nom;
              if (isMain) acc[key + "_is_main"] = true;
            }
          };

          if (cod) update(cod);
          if (seq) update(seq);
          const scod = sanitizeId(cod);
          if (scod && scod !== cod) update(scod);
          
          const ncod = parseInt(cod, 10);
          if (!isNaN(ncod)) update(String(ncod));
        }
        return acc;
      }, {});

      const frotaMap = frotaGeral.reduce((acc: any, v: any) => {
        const seq = String(v.seq_veiculo || "").trim();
        const placa = String(v.num_placa || "").trim();
        const info = `${v.dsc_marca || ""} ${v.dsc_modelo || ""} (${v.num_ano_fabricacao || ""})`.trim();
        if (seq) acc[seq] = info;
        if (placa) acc[placa] = info;
        return acc;
      }, {});

      const frotaPlacaMap = frotaGeral.reduce((acc: any, v: any) => {
        const seq = String(v.seq_veiculo || "").trim();
        const placa = String(v.num_placa || "").trim();
        if (seq && placa) acc[seq] = placa;
        return acc;
      }, {});

      const addResult = (trail: string, item: any, detail: string, id?: string, value?: any, resolution?: string) => {
        // Create a readable formatted string for fullData
        const formattedData = Object.entries(item)
          .filter(([key]) => !key.startsWith('_'))
          .map(([key, val]) => {
            const label = fieldDictionary[key] || key;
            return `➤ ${label}: ${val}`;
          })
          .join('\n');
          
        const seqOrgao = String(item.seq_orgao || "").trim();
        const codOrgao = String(item.cod_orgao || item.cod_unidade_gestora || item.cod_unidade || "").trim();
        
        const getNom = (val: any) => {
          const s = String(val || "").trim();
          return (s && s !== "null" && s !== "undefined" && s !== "Órgão Não Identificado") ? s : null;
        };

        let nomOrgao = orgaoMap[seqOrgao] || orgaoMap[codOrgao] || orgaoMap[sanitizeId(codOrgao)];
        
        if (!nomOrgao) {
          const nCod = parseInt(codOrgao, 10);
          if (!isNaN(nCod)) nomOrgao = orgaoMap[String(nCod)];
        }

        if (!nomOrgao) {
          nomOrgao = getNom(item.nom_orgao) || getNom(item.nom_unidade_gestora) || getNom(item.nom_unidade);
        }
        
        if (!nomOrgao) nomOrgao = "Órgão Não Identificado";

        results.push({
          trail,
          id: id || item.id || item.num_empenho || item.num_licitacao || item.num_contrato || item.num_processo || item.num_documento_credor || '',
          value,
          detail,
          resolution: resolution || "Verificar a conformidade do lançamento com a legislação vigente e retificar se necessário.",
          fullData: formattedData,
          cod_orgao: codOrgao || undefined,
          seq_orgao: seqOrgao || undefined,
          nom_orgao: String(nomOrgao)
        });
      };

      // 1. Fracionamento de Despesa (Lei 14.133/21)
      const itemMap = itensLicitGeral.reduce((acc: any, i: any) => {
        const cod = String(i.cod_item || "").trim();
        if (cod && !acc[cod]) acc[cod] = i.dsc_item;
        return acc;
      }, {});

      const itemDispensa = itensDispensaGeral;
      if (itemDispensa.length) {
        const grouped = itemDispensa.reduce((acc: any, item: any) => {
          const key = `${item.cod_orgao || 'S_ORG'}_${item.cod_item || 'S_COD'}_${item.num_ano_referencia || 'S_ANO'}`;
          if (!acc[key]) acc[key] = { total: 0, item };
          acc[key].total += parseNumber(item.vlr_empenhado);
          return acc;
        }, {});
        Object.entries(grouped).forEach(([key, data]: [string, any]) => {
          if (data.total > 50000) {
            const itemDesc = itemMap[String(data.item.cod_item)] || data.item.dsc_item || "Item Não Identificado";
            addResult(
              "Fracionamento de Despesa", 
              data.item, 
              `O item "${itemDesc}" (Código: ${data.item.cod_item}) acumulou R$ ${data.total.toLocaleString('pt-BR', {minimumFractionDigits: 2, maximumFractionDigits: 2})} em dispensas no exercício, excedendo o limite legal de R$ 50.000,00 (Lei 14.133/21).`, 
              key, 
              data.total,
              "Realizar processo licitatório regular ou justificar o enquadramento em dispensa por valor considerando o planejamento anual de compras."
            );
          }
        });
      }

      // 2. Conflito de Interesses
      const orgaoResp = orgaoRespGeral;
      const quadroSocios = qsaGeral;
      if (orgaoResp.length && quadroSocios.length) {
        const respMap = orgaoResp.reduce((acc: any, r: any) => {
          acc[sanitizeId(r.num_documento_resp)] = r.nom_resp;
          return acc;
        }, {});
        quadroSocios.forEach(socio => {
          const socioId = sanitizeId(socio.num_documento);
          if (socioId && respMap[socioId]) {
            addResult(
              "Conflito de Interesses", 
              socio, 
              `O Sr(a). ${socio.nom_socio || 'N/A'} (CPF/CNPJ ${socio.num_documento}) foi identificado simultaneamente como Sócio da Licitante e como Responsável pelo Órgão (${respMap[socioId]}).`, 
              socio.num_documento,
              undefined,
              "Instaurar processo administrativo para apurar a conduta e garantir a segregação de funções e a impessoalidade nas contratações."
            );
          }
        });
      }

      // 3. Eficiência de Frota
      const gastoFrota = gastoFrotaGeral;
      if (gastoFrota.length) {
        gastoFrota.forEach(g => {
          const mFinal = parseNumber(g.num_marc_final);
          const mInitial = parseNumber(g.num_marc_inicial);
          const quant = parseNumber(g.num_quant_utilizada);
          const tipoGasto = String(g.dsc_tipo_gasto || "").toUpperCase();
          
          if (quant > 0 && tipoGasto.includes("LITRO") && mFinal > mInitial) {
            const km = (mFinal - mInitial) / quant;
            if (km < 3 || km > 25) {
              const vehicleInfo = frotaMap[String(g.seq_veiculo)] || frotaMap[String(g.num_placa)] || "Veículo Não Identificado";
              addResult(
                "Eficiência de Frota", 
                g, 
                `Consumo de ${km.toFixed(2)} km/l fora dos padrões para o veículo ${g.num_placa || g.seq_veiculo || 'N/A'} (${vehicleInfo}). Detalhes: ${quant}L utilizados para percorrer ${(mFinal - mInitial).toFixed(1)}km.`, 
                g.num_placa || String(g.seq_veiculo), 
                km.toFixed(2),
                "Verificar o estado de manutenção do veículo, a correção dos registros de quilometragem e a possível ocorrência de desvios de combustível."
              );
            }
          }
        });
      }

      // 4. Limites LRF (Pessoal)
      const despPessoal = despPessoalGeral;
      const receita = receitasGeral;
      if (despPessoal.length && receita.length) {
        // Group by orgao for LRF
        const orgaos = Array.from(new Set([...despPessoal.map(d => d.cod_orgao), ...receita.map(r => r.cod_orgao)]));
        orgaos.forEach(orgId => {
          const orgDesp = despPessoal.filter(d => d.cod_orgao === orgId);
          const orgRec = receita.filter(r => r.cod_orgao === orgId);
          
          const totalPessoal = orgDesp.reduce((sum, d) => sum + parseNumber(d.vlr_municipio), 0);
          const totalReceita = orgRec.reduce((sum, r) => sum + parseNumber(r.vlr_realizadoateperiodo), 0);
          
          if (totalReceita > 0) {
            const ratio = totalPessoal / totalReceita;
            if (ratio > 0.54) {
              addResult(
                "Limites LRF (Pessoal)", 
                { cod_orgao: orgId }, 
                `Gasto com pessoal (${(ratio * 100).toFixed(2)}%) ultrapassa o limite prudencial da LRF (54%).`, 
                undefined, 
                (ratio * 100).toFixed(2) + "%",
                "Adotar medidas de redução de despesas com pessoal conforme previsto na Lei de Responsabilidade Fiscal, como redução de cargos em comissão e funções de confiança."
              );
            }
          }
        });
      }

      // 5. Certidões Vencidas
      const habLicitacao = habLicitacaoGeral;
      if (habLicitacao.length) {
        habLicitacao.forEach(h => {
          const dHab = parseDate(h.dat_habilitacao);
          const dInss = parseDate(h.dat_val_cert_inss);
          const dFgts = parseDate(h.dat_val_cert_fgts);
          const dCndt = parseDate(h.dat_val_cndt);
          
          if (dHab && ((dInss && dHab > dInss) || (dFgts && dHab > dFgts) || (dCndt && dHab > dCndt))) {
            addResult(
              "Certidões Vencidas", 
              h, 
              `Licitante habilitado com certidão (INSS, FGTS ou CNDT) com validade expirada em relação à data de habilitação.`, 
              h.num_documento,
              undefined,
              "Exigir a apresentação de certidões válidas no momento da habilitação e verificar a regularidade fiscal e trabalhista de forma rigorosa."
            );
          }
        });
      }

      // 6. Restrição à Competitividade
      const licitacao = licitacoesGeral;
      licitacao.forEach(l => {
        const dReceb = parseDate(l.dat_receb_prev_doc);
        const dPub = parseDate(l.dat_pub_edital);
        if (dReceb && dPub) {
          const diffDays = (dReceb.getTime() - dPub.getTime()) / (1000 * 3600 * 24);
          if (diffDays < 8 && diffDays >= 0) {
            addResult(
              "Restrição à Competitividade", 
              l, 
              `Intervalo de ${diffDays.toFixed(2)} dias entre publicação e abertura é inferior ao mínimo legal, prejudicando a ampla competitividade.`, 
              l.num_licitacao, 
              diffDays.toFixed(2),
              "Republicar o edital com a reabertura do prazo legal para garantir que todos os interessados tenham tempo hábil para preparar suas propostas."
            );
          }
        }
      });

      // 7. Caronas em Atas
      const regAdesao = regAdesaoGeral;
      regAdesao.forEach(r => {
        if (compareIds(r.dsc_nat_processo, 2)) {
          addResult(
            "Caronas em Atas", 
            r, 
            `Adesão à Ata de Registro de Preços por Órgão Não Participante (Carona) identificada.`, 
            r.num_adesao,
            undefined,
            "Justificar a vantagem econômica da adesão e garantir que os limites de quantitativos previstos na legislação para caronas sejam rigorosamente observados."
          );
        }
      });

      // 8. Integridade de Notas
      const notas = notasFiscaisGeral;
      const itemNfs = itemNfsGeral;
      if (notas.length && itemNfs.length) {
        notas.forEach(n => {
          const items = itemNfs.filter(i => compareIds(i.num_nota, n.num_nota));
          if (items.length > 0) {
            const sumItems = items.reduce((sum, i) => sum + (parseNumber(i.vlr_item) * parseNumber(i.num_quant_item)), 0);
            const vlrBruto = parseNumber(n.vlr_bruto);
            if (Math.abs(vlrBruto - sumItems) > 0.05) {
              addResult(
                "Integridade de Notas", 
                n, 
                `Divergência de valores na Nota Fiscal: Valor Bruto (R$ ${vlrBruto.toFixed(2)}) difere da soma dos itens (R$ ${sumItems.toFixed(2)}).`, 
                n.num_nota,
                undefined,
                "Solicitar a correção da nota fiscal junto ao fornecedor ou realizar o estorno do lançamento incorreto, garantindo a fidedignidade dos dados contábeis."
              );
            }
          }
        });
      }

      // 11. Rastreabilidade Financeira
      const movPagamento = movPagamentoGeral;
      if (movPagamento.length) {
        movPagamento.forEach(m => {
          if (compareIds(m.dsc_tipo_doc, 5)) {
            addResult(
              "Rastreabilidade Financeira", 
              m, 
              `Pagamento realizado em espécie (dinheiro), dificultando a rastreabilidade do fluxo financeiro.`, 
              m.num_empenho,
              undefined,
              "Priorizar pagamentos via transferência bancária ou ordem bancária eletrônica, conforme normas de transparência e controle de gastos públicos."
            );
          }
        });
      }

      // 12. Itens Genéricos
      const itemLicitacao = itensLicitGeral;
      const genericRegex = /diversos|conforme pedido|conforme anexo|item generico/i;
      itemLicitacao.forEach(item => {
        if (item.dsc_item && genericRegex.test(String(item.dsc_item))) {
          addResult(
            "Itens Genéricos", 
            item, 
            `Objeto com descrição genérica ("${item.dsc_item}"), o que pode dificultar a aferição de preços de mercado.`, 
            item.cod_item,
            undefined,
            "Especificar detalhadamente o objeto da contratação no edital e nos documentos de empenho, evitando termos vagos que impossibilitem o controle de preços."
          );
        }
      });

      // 13. Pagamento Antecipado
      const pagamentos = pagamentosGeral;
      const liquidacoes = liquidacoesGeral;
      if (pagamentos.length && liquidacoes.length) {
        pagamentos.forEach(p => {
          const liq = liquidacoes.find(l => compareIds(l.num_empenho, p.num_empenho));
          if (liq) {
            const dPag = parseDate(p.dat_pagamento);
            const dLiq = parseDate(liq.dat_liquidacao);
            if (dPag && dLiq && dPag < dLiq) {
              addResult(
                "Pagamento Antecipado", 
                p, 
                `Pagamento realizado em ${p.dat_pagamento} antes da liquidação da despesa (${liq.dat_liquidacao}), violando a Lei 4.320/64.`, 
                p.num_empenho,
                undefined,
                "Respeitar os estágios da despesa pública (empenho, liquidação e pagamento), garantindo que o pagamento ocorra apenas após a efetiva entrega do bem ou serviço."
              );
            }
          }
        });
      }

      // 14. Diárias Excessivas
      const empenhos = empenhosGeral;
      const diariaEmpenhos = empenhos.filter(e => String(e.dsc_objeto).toLowerCase().includes("diária") || String(e.dsc_objeto).toLowerCase().includes("diaria"));
      const diariaMap = diariaEmpenhos.reduce((acc: any, e: any) => {
        const key = `${e.cod_orgao || 'S_ORG'}_${e.num_documento_credor || 'S_DOC'}`;
        if (!acc[key]) acc[key] = { total: 0, item: e };
        acc[key].total += parseNumber(e.vlr_empenho);
        return acc;
      }, {});
      Object.entries(diariaMap).forEach(([key, data]: [string, any]) => {
        if (data.total > 15000) {
          addResult(
            "Diárias Excessivas", 
            data.item, 
            `Acúmulo de R$ ${data.total.toLocaleString('pt-BR', {minimumFractionDigits: 2, maximumFractionDigits: 2})} em diárias para o mesmo CPF/CNPJ no exercício.`, 
            data.item.num_documento_credor, 
            data.total,
            "Justificar a necessidade das viagens e verificar se os valores pagos estão de acordo com a regulamentação municipal e os princípios da razoabilidade."
          );
        }
      });

      // 15. Ausência de Publicidade
      licitacao.forEach(l => {
        if (!l.dat_pub_edital || l.dat_pub_edital === "") {
          addResult(
            "Ausência de Publicidade", 
            l, 
            `Licitação identificada sem registro de data de publicação do edital no SICOM.`, 
            l.num_licitacao,
            undefined,
            "Garantir a publicação oficial dos editais de licitação nos meios previstos em lei para assegurar a transparência e a ampla participação de licitantes."
          );
        }
      });

      // 16. Abastecimento em Finais de Semana
      const abastecimentos = gastoFrotaGeral;
      abastecimentos.forEach(a => {
        const dAbast = parseDate(a.dat_abastecimento);
        if (dAbast) {
          const day = dAbast.getDay(); // 0 = Sunday, 6 = Saturday
          if (day === 0 || day === 6) {
            addResult(
              "Abastecimento Fim de Semana", 
              a, 
              `Abastecimento realizado no veículo ${a.num_placa || 'N/A'} em um ${day === 0 ? 'Domingo' : 'Sábado'} (${a.dat_abastecimento}).`, 
              a.num_placa, 
              a.vlr_total,
              "Verificar se o abastecimento foi devidamente autorizado para fins de serviço público essencial que justifique a utilização do veículo no final de semana."
            );
          }
        }
      });

      // 17. Aditivos Exorbitantes
      const termoContrato = termosContratoGeral;
      const contrato = contratosGeral;
      if (termoContrato.length && contrato.length) {
        contrato.forEach(c => {
          const aditivos = termoContrato.filter(t => compareIds(t.num_contrato, c.num_contrato));
          const totalAditivos = aditivos.reduce((sum, t) => sum + parseNumber(t.vlr_recurso), 0);
          const vlrOriginal = parseNumber(c.vlr_contrato);
          if (vlrOriginal > 0 && totalAditivos > (vlrOriginal * 0.25)) {
            addResult(
              "Aditivos Exorbitantes", 
              c, 
              `Soma dos termos aditivos (R$ ${totalAditivos.toLocaleString('pt-BR', {minimumFractionDigits: 2, maximumFractionDigits: 2})}) ultrapassa 25% do valor original do contrato (R$ ${vlrOriginal.toLocaleString('pt-BR', {minimumFractionDigits: 2, maximumFractionDigits: 2})}).`, 
              c.num_contrato, 
              totalAditivos,
              "Observar os limites legais para aditamentos contratuais (25% para acréscimos e supressões) e justificar tecnicamente qualquer alteração no objeto original."
            );
          }
        });
      }

      // 18. Segregação de Funções
      const liquidacao = liquidacoesGeral;
      const pagamento = pagamentosDespesaGeral;
      if (liquidacao.length && pagamento.length) {
        liquidacao.forEach(l => {
          const p = pagamento.find(pg => compareIds(pg.num_empenho, l.num_empenho));
          if (p && l.nom_liquidante && p.nom_resp && String(l.nom_liquidante).trim() === String(p.nom_resp).trim()) {
            addResult(
              "Segregação de Funções", 
              l, 
              `O servidor ${l.nom_liquidante} atuou simultaneamente na liquidação e no pagamento, violando o princípio da segregação de funções.`, 
              l.num_empenho,
              undefined,
              "Designar servidores distintos para as fases de liquidação e pagamento, garantindo o controle cruzado e a integridade dos processos de despesa."
            );
          }
        });
      }

      // 19. Acúmulo de Cargos (Simplificado)
      const pessoal = pessoalGeral;
      if (pessoal.length) {
        const cpfMap = pessoal.reduce((acc: any, p: any) => {
          const cpf = sanitizeId(p.num_cpf);
          if (cpf) {
            if (!acc[cpf]) acc[cpf] = [];
            acc[cpf].push(p);
          }
          return acc;
        }, {});
        Object.entries(cpfMap).forEach(([cpf, items]: [string, any]) => {
          if (items.length > 1) {
            addResult(
              "Acúmulo de Cargos", 
              items[0], 
              `CPF ${cpf} identificado com ${items.length} vínculos ativos no mesmo período.`, 
              cpf,
              undefined,
              "Verificar a compatibilidade de horários e a legalidade do acúmulo de cargos conforme as exceções previstas na Constituição Federal."
            );
          }
        });
      }

      // 18. Empenho sem Licitação/Dispensa
      const empenhosAll = empenhosGeral;
      empenhosAll.forEach(e => {
        if (!e.num_licitacao && !e.num_processo_dispensa && parseNumber(e.vlr_empenho) > 17600) {
          const vlr = parseNumber(e.vlr_empenho);
          addResult(
            "Empenho sem Licitação", 
            e, 
            `Empenho de valor elevado (R$ ${vlr.toLocaleString('pt-BR', {minimumFractionDigits: 2})}) sem indicação de processo licitatório ou dispensa.`, 
            e.num_empenho, 
            e.vlr_empenho,
            "Vincular o empenho ao respectivo processo licitatório ou de dispensa/inexigibilidade, garantindo a regularidade da contratação pública."
          );
        }
      });

      // 19. Divergência de Datas (Empenho vs Contrato)
      const contratosAll = contratosGeral;
      if (empenhosAll.length && contratosAll.length) {
        empenhosAll.forEach(e => {
          const c = contratosAll.find(con => compareIds(con.num_contrato, e.num_contrato));
          if (c) {
            const dEmp = parseDate(e.dat_empenho);
            const dCon = parseDate(c.dat_contrato);
            if (dEmp && dCon && dEmp < dCon) {
              addResult(
                "Divergência de Datas", 
                e, 
                `Data do empenho (${e.dat_empenho}) é anterior à data de assinatura do contrato (${c.dat_contrato}).`, 
                e.num_empenho,
                undefined,
                "Ajustar os registros para que o empenho ocorra após ou simultaneamente à assinatura do contrato, respeitando a ordem cronológica dos atos administrativos."
              );
            }
          }
        });
      }

      // 20. Pagamentos Duplicados em Órgãos Diferentes
      const allPagamentos = pagamentosGeral;
      if (allPagamentos.length) {
        const pagMap = allPagamentos.reduce((acc: any, p: any) => {
          const key = `${sanitizeId(p.num_documento_credor)}_${parseNumber(p.vlr_pagamento).toFixed(2)}_${p.dat_pagamento}`;
          if (!acc[key]) acc[key] = [];
          acc[key].push(p);
          return acc;
        }, {});

        Object.entries(pagMap).forEach(([key, items]: [string, any]) => {
          if (items.length > 1) {
            const organs = new Set(items.map((i: any) => i.cod_orgao));
            if (organs.size > 1) {
              addResult(
                "Pagamentos Duplicados (Órgãos Dif.)",
                items[0],
                `Identificado pagamento de mesmo valor (R$ ${parseNumber(items[0].vlr_pagamento).toLocaleString('pt-BR', {minimumFractionDigits: 2})}) e data (${items[0].dat_pagamento}) para o mesmo credor em ${organs.size} órgãos diferentes.`,
                items[0].num_documento_credor,
                items[0].vlr_pagamento,
                "Verificar se houve duplicidade indevida de pagamento ou se tratam de serviços distintos prestados a diferentes unidades gestoras."
              );
            }
          }
        });
      }

      // 21. Contratos Expirados sem Aditivos
      const contratos = contratosGeral;
      const aditivos = termosContratoGeral;
      const today = new Date();
      
      contratos.forEach(c => {
        const dAssinatura = parseDate(c.dat_assinatura);
        const prazo = parseInt(c.num_prazo_execucao, 10);
        if (dAssinatura && !isNaN(prazo)) {
          const dFim = new Date(dAssinatura);
          dFim.setDate(dFim.getDate() + prazo);
          
          if (dFim < today) {
            const hasAditivo = aditivos.some(a => compareIds(a.num_contrato, c.num_contrato));
            if (!hasAditivo) {
              addResult(
                "Contrato Expirado sem Aditivo",
                c,
                `Contrato com prazo de execução vencido em ${dFim.toLocaleDateString('pt-BR')} sem registro de termo aditivo de prorrogação.`,
                c.num_contrato,
                c.vlr_contrato,
                "Regularizar a situação contratual através de termo aditivo, se ainda houver interesse público, ou proceder ao encerramento formal do contrato."
              );
            }
          }
        }
      });

      // 22. Acompanhamento de Manutenção de Veículos
      const manutencoesFromGasto = gastoFrotaGeral
        .filter((g: any) => String(g.dsc_tipo_gasto || "").includes("SERVIÇO") || String(g.dsc_tipo_gasto || "").includes("MANUTENÇÃO"))
        .map((g: any) => ({
          ...g,
          num_placa: frotaPlacaMap[String(g.seq_veiculo)] || `VEIC-${g.seq_veiculo}`,
          dat_manutencao: `${g.num_ano_referencia}-${String(g.num_mes_referencia).padStart(2, '0')}-01`,
          vlr_manutencao: g.vlr_gasto,
          num_marc_odometro: g.num_marc_final
        }));

      const allManutencoes = [...manutencaoFrotaGeral, ...manutencoesFromGasto];

      if (allManutencoes.length) {
        // Group by vehicle to check frequency
        const vehicleMap = allManutencoes.reduce((acc: any, m: any) => {
          const key = sanitizeId(m.num_placa);
          if (!acc[key]) acc[key] = [];
          acc[key].push(m);
          return acc;
        }, {});

        Object.entries(vehicleMap).forEach(([placa, items]: [string, any]) => {
          // Sort by date
          const sorted = items.sort((a: any, b: any) => {
            const dA = parseDate(a.dat_manutencao) || new Date(0);
            const dB = parseDate(b.dat_manutencao) || new Date(0);
            return dA.getTime() - dB.getTime();
          });

          // Check for high frequency (more than 3 in 30 days)
          for (let i = 0; i < sorted.length - 2; i++) {
            const d1 = parseDate(sorted[i].dat_manutencao);
            const d3 = parseDate(sorted[i+2].dat_manutencao);
            if (d1 && d3) {
              const diffDays = (d3.getTime() - d1.getTime()) / (1000 * 3600 * 24);
              if (diffDays <= 30) {
                addResult(
                  "Acompanhamento de Manutenção de Veículos (Frequência)",
                  sorted[i+2],
                  `Veículo ${placa} identificado com 3 manutenções em um intervalo de ${diffDays.toFixed(0)} dias.`,
                  placa,
                  undefined,
                  "Avaliar a qualidade das peças e serviços realizados, bem como a necessidade de substituição do veículo por obsolescência ou custo antieconômico."
                );
                break; // Alert once per vehicle in this period
              }
            }
          }

          // Check for high individual cost (e.g., > R$ 10.000,00)
          sorted.forEach((m: any) => {
            const vlr = parseNumber(m.vlr_manutencao);
            if (vlr > 10000) {
              addResult(
                "Acompanhamento de Manutenção de Veículos (Alto Custo)",
                m,
                `Manutenção individual de valor elevado (R$ ${vlr.toLocaleString('pt-BR', {minimumFractionDigits: 2})}) para o veículo ${placa}.`,
                placa,
                vlr,
                "Justificar tecnicamente a necessidade do reparo e realizar pesquisa de preços de mercado para garantir a economicidade da despesa."
              );
            }
          });

          // Check for odometer inconsistency
          for (let i = 0; i < sorted.length - 1; i++) {
            const o1 = parseNumber(sorted[i].num_marc_odometro);
            const o2 = parseNumber(sorted[i+1].num_marc_odometro);
            if (o1 > 0 && o2 > 0 && o2 < o1) {
              addResult(
                "Acompanhamento de Manutenção de Veículos (Inconsistência de Odômetro)",
                sorted[i+1],
                `Odômetro registrado (${o2}) é inferior ao registro da manutenção anterior (${o1}) para o veículo ${placa}.`,
                placa,
                undefined,
                "Verificar se houve erro de digitação no registro do odômetro ou se houve substituição do painel de instrumentos sem a devida anotação."
              );
            }
          }
        });
      }

      // 23. Concentração de Contratações (Indício de Favorecimento)
      const empenhosFav = empenhosGeral;
      if (empenhosFav.length) {
        const totalEmpenhado = empenhosFav.reduce((sum, e) => sum + parseNumber(e.vlr_empenho), 0);
        const credMap = empenhosFav.reduce((acc: any, e: any) => {
          const key = sanitizeId(e.num_documento_credor);
          if (!acc[key]) acc[key] = { total: 0, nom: e.nom_credor, item: e };
          acc[key].total += parseNumber(e.vlr_empenho);
          return acc;
        }, {});

        Object.entries(credMap).forEach(([cnpj, data]: [string, any]) => {
          const percent = (data.total / totalEmpenhado) * 100;
          if (percent > 30 && totalEmpenhado > 100000) { // More than 30% of total budget
            addResult(
              "Concentração de Contratações",
              data.item,
              `O credor ${data.nom} concentra ${(percent).toFixed(2)}% do volume total empenhado (R$ ${data.total.toLocaleString('pt-BR', {minimumFractionDigits: 2})}).`,
              cnpj,
              data.total,
              "Analisar a competitividade dos certames vencidos por este credor e verificar se há indícios de direcionamento ou restrição à participação de outros fornecedores."
            );
          }
        });
      }

      // 24. Cartelização (Sócios em Comum na mesma Licitação)
      const qsa = qsaGeral;
      if (qsa.length) {
        const licMap = qsa.reduce((acc: any, s: any) => {
          const licId = sanitizeId(s.num_licitacao);
          const socioId = sanitizeId(s.num_documento);
          const licitanteId = sanitizeId(s.num_documento_licitante);
          
          if (!acc[licId]) acc[licId] = {};
          if (!acc[licId][socioId]) acc[licId][socioId] = new Set();
          acc[licId][socioId].add(licitanteId);
          return acc;
        }, {});

        Object.entries(licMap).forEach(([licId, socios]: [string, any]) => {
          Object.entries(socios).forEach(([socioId, licitantes]: [string, any]) => {
            if (licitantes.size > 1) {
              addResult(
                "Cartelização (Sócios Comuns)",
                { num_licitacao: licId, num_documento: socioId },
                `Identificadas ${licitantes.size} empresas com o mesmo sócio (CPF/CNPJ: ${socioId}) participando da licitação ${licId}.`,
                licId,
                undefined,
                "Investigar a relação entre as empresas participantes e anular o certame se confirmada a colusão ou fraude à competitividade."
              );
            }
          });
        });
      }

      // 25. Emergência Fabricada (Dispensas Recorrentes)
      const dispensas = dispensasGeral;
      if (dispensas.length) {
        const emergencias = dispensas.filter(d => 
          String(d.dsc_objeto).toLowerCase().includes("emerg") || 
          String(d.dsc_fundamento_legal).toLowerCase().includes("emerg")
        );
        
        const objMap = emergencias.reduce((acc: any, d: any) => {
          const key = `${sanitizeId(d.cod_orgao)}_${String(d.dsc_objeto).substring(0, 30).toLowerCase()}`;
          if (!acc[key]) acc[key] = [];
          acc[key].push(d);
          return acc;
        }, {});

        Object.entries(objMap).forEach(([key, items]: [string, any]) => {
          if (items.length > 2) {
            addResult(
              "Emergência Fabricada",
              items[0],
              `Identificadas ${items.length} dispensas por emergência para objetos similares no mesmo exercício, sugerindo falta de planejamento.`,
              items[0].num_processo,
              undefined,
              "Realizar planejamento anual de contratações para evitar o uso recorrente de dispensas emergenciais para necessidades previsíveis."
            );
          }
        });
      }

      // 26. Teto Salarial (Acúmulo de Vencimentos)
      const pessoalAll = pessoalGeral;
      if (pessoalAll.length) {
        const teto = 35000; // Exemplo de teto (ajustável)
        const cpfVencMap = pessoalAll.reduce((acc: any, p: any) => {
          const cpf = sanitizeId(p.num_cpf);
          if (!acc[cpf]) acc[cpf] = { total: 0, nom: p.nom_servidor, item: p, orgaos: new Set() };
          acc[cpf].total += parseNumber(p.vlr_bruto || p.vlr_municipio);
          acc[cpf].orgaos.add(orgaoMap[String(p.cod_orgao || "").trim()] || orgaoMap[String(p.seq_orgao || "").trim()] || p.nom_orgao || p.cod_orgao);
          return acc;
        }, {});

        Object.entries(cpfVencMap).forEach(([cpf, data]: [string, any]) => {
          if (data.total > teto) {
            addResult(
              "Teto Salarial Ultrapassado",
              data.item,
              `Servidor ${data.nom} possui remuneração total de R$ ${data.total.toLocaleString('pt-BR', {minimumFractionDigits: 2})} (soma de ${data.orgaos.size} vínculos), ultrapassando o teto estimado.`,
              cpf,
              data.total,
              "Aplicar o abate-teto sobre a remuneração que exceder o limite constitucional e verificar a legalidade do acúmulo de cargos."
            );
          }
        });
      }

      // 27. Divergência de Quantitativos (Licitado vs Empenhado)
      const itensLicit = itensLicitGeral;
      const itensEmp = itensEmpenhoGeral;
      if (itensLicit.length && itensEmp.length) {
        const licitQuantMap = itensLicit.reduce((acc: any, i: any) => {
          const key = `${sanitizeId(i.num_licitacao)}_${sanitizeId(i.cod_item)}`;
          acc[key] = (acc[key] || 0) + parseNumber(i.num_quant_item);
          return acc;
        }, {});

        const empQuantMap = itensEmp.reduce((acc: any, i: any) => {
          const key = `${sanitizeId(i.num_licitacao)}_${sanitizeId(i.cod_item)}`;
          if (i.num_licitacao) {
            acc[key] = (acc[key] || 0) + parseNumber(i.num_quant_item);
          }
          return acc;
        }, {});

        Object.entries(empQuantMap).forEach(([key, qEmp]: [string, any]) => {
          const qLic = licitQuantMap[key];
          if (qLic && qEmp > qLic) {
            const [licId, itemId] = key.split('_');
            addResult(
              "Divergência de Quantitativos",
              { num_licitacao: licId, cod_item: itemId },
              `Quantidade empenhada (${qEmp}) é superior à quantidade licitada (${qLic}) para o item ${itemId} na licitação ${licId}.`,
              licId,
              undefined,
              "Limitar os empenhos às quantidades efetivamente licitadas e adjudicadas, evitando a realização de despesas sem o devido amparo legal."
            );
          }
        });
      }

      // 28. Fundeb 70% (Profissionais do Magistério)
      const empenhosEduc = empenhosGeral;
      const receitasEduc = receitasGeral;
      const empenhosSaude = empenhosGeral;
      const receitasSaude = receitasGeral;

      if (empenhosEduc.length && receitasEduc.length) {
        const fundebReceita = receitasEduc.filter(r => 
          String(r.cod_natureza_receita).startsWith("1715") || 
          String(r.cod_natureza_receita).startsWith("175")
        ).reduce((sum, r) => sum + parseNumber(r.vlr_realizadoateperiodo), 0);

        const fundebPessoal = empenhosEduc.filter(e => 
          (String(e.cod_funcao) === "12") && 
          (String(e.cod_elemento_despesa).startsWith("3190")) && // Pessoal
          (String(e.cod_fonte_recurso).includes("113") || String(e.cod_fonte_recurso).includes("114")) // Fontes Fundeb
        ).reduce((sum, e) => sum + parseNumber(e.vlr_empenho), 0);

        if (fundebReceita > 0) {
          const ratio = fundebPessoal / fundebReceita;
          if (ratio < 0.70) {
            addResult(
              "Fundeb 70% (Magistério)",
              { total_receita: fundebReceita, total_pessoal: fundebPessoal },
              `Gasto com profissionais do magistério (${(ratio * 100).toFixed(2)}%) está abaixo do limite legal de 70% dos recursos do Fundeb.`,
              undefined,
              (ratio * 100).toFixed(2) + "%",
              "Aumentar o investimento na remuneração dos profissionais da educação básica para atingir o limite mínimo de 70% exigido pela legislação do Fundeb."
            );
          }
        }
      }

      // 29. MDE 25% (Manutenção e Desenvolvimento do Ensino)
      if (empenhosEduc.length && receitasEduc.length) {
        const impostosReceita = receitasEduc.filter(r => 
          String(r.cod_natureza_receita).startsWith("11") || // Impostos
          String(r.cod_natureza_receita).startsWith("17")    // Transferências
        ).reduce((sum, r) => sum + parseNumber(r.vlr_realizadoateperiodo), 0);

        const mdeDespesa = empenhosEduc.filter(e => 
          String(e.cod_funcao) === "12" // Educação
        ).reduce((sum, e) => sum + parseNumber(e.vlr_empenho), 0);

        if (impostosReceita > 0) {
          const ratio = mdeDespesa / impostosReceita;
          if (ratio < 0.25) {
            addResult(
              "MDE 25% (Educação)",
              { total_receita: impostosReceita, total_mde: mdeDespesa },
              `Investimento em Educação (${(ratio * 100).toFixed(2)}%) está abaixo do limite constitucional de 25% da receita de impostos.`,
              undefined,
              (ratio * 100).toFixed(2) + "%",
              "Elevar a aplicação de recursos em Manutenção e Desenvolvimento do Ensino para cumprir o preceito constitucional de 25% da receita de impostos."
            );
          }
        }
      }

      // 30. Concentração em Merenda Escolar (PNAE)
      const empenhosMerenda = empenhosEduc.filter(e => 
        String(e.dsc_objeto).toLowerCase().includes("merenda") || 
        String(e.dsc_objeto).toLowerCase().includes("alimentação escolar")
      );
      if (empenhosMerenda.length) {
        const totalMerenda = empenhosMerenda.reduce((sum, e) => sum + parseNumber(e.vlr_empenho), 0);
        const credMerendaMap = empenhosMerenda.reduce((acc: any, e: any) => {
          const key = sanitizeId(e.num_documento_credor);
          if (!acc[key]) acc[key] = { total: 0, nom: e.nom_credor, item: e };
          acc[key].total += parseNumber(e.vlr_empenho);
          return acc;
        }, {});

        Object.entries(credMerendaMap).forEach(([cnpj, data]: [string, any]) => {
          const percent = (data.total / totalMerenda) * 100;
          if (percent > 60 && totalMerenda > 50000) {
            addResult(
              "Concentração em Merenda Escolar",
              data.item,
              `O fornecedor ${data.nom} concentra ${(percent).toFixed(2)}% do gasto total com merenda escolar (R$ ${data.total.toLocaleString('pt-BR', {minimumFractionDigits: 2})}).`,
              cnpj,
              data.total,
              "Verificar a competitividade das licitações para fornecimento de merenda e garantir que não haja monopólio ou favorecimento de fornecedores."
            );
          }
        });
      }

      // 31. Manutenção de Transporte Escolar
      const manutencoesEscolar = manutencaoFrotaGeral;
      if (manutencoesEscolar.length) {
        const escolarManut = manutencoesEscolar.filter(m => 
          String(m.dsc_tipo_veiculo).toLowerCase().includes("escolar") || 
          String(m.dsc_objeto).toLowerCase().includes("escolar")
        );
        
        escolarManut.forEach(m => {
          const vlr = parseNumber(m.vlr_manutencao);
          if (vlr > 10000) {
            addResult(
              "Manutenção Transporte Escolar",
              m,
              `Manutenção de alto custo (R$ ${vlr.toLocaleString('pt-BR', {minimumFractionDigits: 2})}) identificada em veículo de transporte escolar (Placa: ${m.num_placa}).`,
              m.num_placa,
              vlr,
              "Auditar as ordens de serviço e as peças substituídas no transporte escolar para garantir a segurança dos alunos e a economicidade dos reparos."
            );
          }
        });
      }

      // 32. ASPS 15% (Ações e Serviços Públicos de Saúde)
      if (empenhosSaude.length && receitasSaude.length) {
        const impostosReceita = receitasSaude.filter(r => 
          String(r.cod_natureza_receita).startsWith("11") || // Impostos
          String(r.cod_natureza_receita).startsWith("17")    // Transferências
        ).reduce((sum, r) => sum + parseNumber(r.vlr_realizadoateperiodo), 0);

        const aspsDespesa = empenhosSaude.filter(e => 
          String(e.cod_funcao) === "10" // Saúde
        ).reduce((sum, e) => sum + parseNumber(e.vlr_empenho), 0);

        if (impostosReceita > 0) {
          const ratio = aspsDespesa / impostosReceita;
          if (ratio < 0.15) {
            addResult(
              "ASPS 15% (Saúde)",
              { total_receita: impostosReceita, total_asps: aspsDespesa },
              `Investimento em Saúde (${(ratio * 100).toFixed(2)}%) está abaixo do limite constitucional de 15% da receita de impostos.`,
              undefined,
              (ratio * 100).toFixed(2) + "%",
              "Elevar a aplicação de recursos em Ações e Serviços Públicos de Saúde para cumprir o preceito constitucional de 15% da receita de impostos."
            );
          }
        }
      }

      // 33. Concentração em Medicamentos
      const empenhosMed = empenhosSaude.filter(e => 
        String(e.dsc_objeto).toLowerCase().includes("medicamento") || 
        String(e.dsc_objeto).toLowerCase().includes("remedio")
      );
      if (empenhosMed.length) {
        const totalMed = empenhosMed.reduce((sum, e) => sum + parseNumber(e.vlr_empenho), 0);
        const credMedMap = empenhosMed.reduce((acc: any, e: any) => {
          const key = sanitizeId(e.num_documento_credor);
          if (!acc[key]) acc[key] = { total: 0, nom: e.nom_credor, item: e };
          acc[key].total += parseNumber(e.vlr_empenho);
          return acc;
        }, {});

        Object.entries(credMedMap).forEach(([cnpj, data]: [string, any]) => {
          const percent = (data.total / totalMed) * 100;
          if (percent > 50 && totalMed > 100000) {
            addResult(
              "Concentração em Medicamentos",
              data.item,
              `O fornecedor ${data.nom} concentra ${(percent).toFixed(2)}% do gasto total com medicamentos (R$ ${data.total.toLocaleString('pt-BR', {minimumFractionDigits: 2})}).`,
              cnpj,
              data.total,
              "Verificar a competitividade das licitações para compra de medicamentos e garantir que não haja monopólio ou favorecimento de fornecedores."
            );
          }
        });
      }

      // 34. Manutenção de Ambulâncias
      const manutencoesSaude = manutencaoFrotaGeral;
      if (manutencoesSaude.length) {
        const ambulanciaManut = manutencoesSaude.filter(m => 
          String(m.dsc_tipo_veiculo).toLowerCase().includes("ambulancia") || 
          String(m.dsc_objeto).toLowerCase().includes("ambulancia")
        );
        
        ambulanciaManut.forEach(m => {
          const vlr = parseNumber(m.vlr_manutencao);
          if (vlr > 12000) {
            addResult(
              "Manutenção de Ambulância",
              m,
              `Manutenção de alto custo (R$ ${vlr.toLocaleString('pt-BR', {minimumFractionDigits: 2})}) identificada em ambulância (Placa: ${m.num_placa}).`,
              m.num_placa,
              vlr,
              "Auditar as ordens de serviço e as peças substituídas nas ambulâncias para garantir a disponibilidade da frota e a economicidade dos reparos."
            );
          }
        });
      }

      // 35. Plantões Médicos Excessivos
      const empenhosPlantao = empenhosSaude.filter(e => 
        String(e.dsc_objeto).toLowerCase().includes("plantão") || 
        String(e.dsc_objeto).toLowerCase().includes("plantao")
      );
      if (empenhosPlantao.length) {
        const plantaoMap = empenhosPlantao.reduce((acc: any, e: any) => {
          const key = sanitizeId(e.num_documento_credor);
          if (!acc[key]) acc[key] = { total: 0, nom: e.nom_credor, item: e, count: 0 };
          acc[key].total += parseNumber(e.vlr_empenho);
          acc[key].count += 1;
          return acc;
        }, {});

        Object.entries(plantaoMap).forEach(([cnpj, data]: [string, any]) => {
          if (data.total > 25000) { // Valor alto para um único profissional/mês
            addResult(
              "Plantões Médicos Excessivos",
              data.item,
              `O profissional/empresa ${data.nom} recebeu R$ ${data.total.toLocaleString('pt-BR', {minimumFractionDigits: 2})} em plantões no período, valor acima da média de mercado.`,
              cnpj,
              data.total,
              "Verificar a compatibilidade da carga horária trabalhada com o valor pago e se os plantões foram efetivamente realizados conforme a escala de serviço."
            );
          }
        });
      }

      // 36. Abastecimento Superior à Capacidade (Frota)
      const abastecimentosAll = gastoFrotaGeral;
      abastecimentosAll.forEach(a => {
        const quant = parseNumber(a.num_quant_utilizada);
        if (quant > 100) { // Limite genérico de 100 litros para veículos leves/médios
          addResult(
            "Abastecimento Suspeito",
            a,
            `Abastecimento de ${quant.toFixed(2)} litros no veículo ${a.num_placa || 'N/A'} excede a capacidade média de tanques convencionais.`,
            a.num_placa,
            quant,
            "Verificar se o volume abastecido é compatível com a capacidade do tanque do veículo e se houve erro de registro ou desvio de combustível."
          );
        }
      });

      // 37. Licitação with Participante Único (Licitação)
      const licitacoesAll = licitacoesGeral;
      const habilitacoesAll = habLicitacaoGeral;
      if (licitacoesAll.length && habilitacoesAll.length) {
        licitacoesAll.forEach(l => {
          const habs = habilitacoesAll.filter(h => compareIds(h.num_licitacao, l.num_licitacao));
          if (habs.length === 1) {
            addResult(
              "Licitação com Participante Único",
              l,
              `Apenas um licitante foi habilitado para o certame ${l.num_licitacao}, o que pode indicar restrição à competitividade.`,
              l.num_licitacao,
              undefined,
              "Avaliar as causas da baixa participação e verificar se as exigências do edital foram excessivas ou direcionadas."
            );
          }
        });
      }

      // 38. Despesas com Publicidade (Despesa)
      const empenhosPub = empenhosGeral;
      const pubEmpenhos = empenhosPub.filter(e => 
        String(e.dsc_objeto).toLowerCase().includes("publicidade") || 
        String(e.dsc_objeto).toLowerCase().includes("propaganda") ||
        String(e.cod_subfuncao) === "131" // Comunicação Social
      );
      if (pubEmpenhos.length) {
        const totalPub = pubEmpenhos.reduce((sum, e) => sum + parseNumber(e.vlr_empenho), 0);
        if (totalPub > 50000) {
          addResult(
            "Gasto com Publicidade",
            pubEmpenhos[0],
            `Volume total de gastos com publicidade e propaganda (R$ ${totalPub.toLocaleString('pt-BR', {minimumFractionDigits: 2})}) requer acompanhamento especial.`,
            undefined,
            totalPub,
            "Garantir que os gastos com publicidade tenham caráter estritamente educativo, informativo ou de orientação social, conforme a Constituição."
          );
        }
      }

      // 39. Horas Extras Excessivas (Pessoal)
      const pessoalExtra = pessoalGeral;
      if (pessoalExtra.length) {
        pessoalExtra.forEach(p => {
          const vlrBruto = parseNumber(p.vlr_bruto);
          const vlrExtra = parseNumber(p.vlr_horas_extras || 0);
          if (vlrBruto > 0 && vlrExtra > (vlrBruto * 0.5)) {
            addResult(
              "Horas Extras Excessivas",
              p,
              `Servidor ${p.nom_servidor} recebeu R$ ${vlrExtra.toLocaleString('pt-BR', {minimumFractionDigits: 2})} em horas extras, excedendo 50% do seu salário bruto.`,
              p.num_cpf,
              vlrExtra,
              "Verificar a real necessidade da prestação de serviço extraordinário e se houve autorização prévia e controle efetivo da jornada."
            );
          }
        });
      }

      // 40. Renúncia de Receita (Receita)
      const receitasRenuncia = receitasGeral;
      receitasRenuncia.forEach(r => {
        const vlrRenuncia = parseNumber(r.vlr_renuncia || 0);
        if (vlrRenuncia > 10000) {
          addResult(
            "Renúncia de Receita",
            r,
            `Identificada renúncia de receita no valor de R$ ${vlrRenuncia.toLocaleString('pt-BR', {minimumFractionDigits: 2})} para a natureza ${r.cod_natureza_receita}.`,
            r.cod_natureza_receita,
            vlrRenuncia,
            "Verificar se a renúncia de receita foi acompanhada da estimativa de impacto orçamentário-financeiro exigida pela LRF."
          );
        }
      });

      // 41. Fracionamento de Medicamentos (Saúde)
      const itemDispensaMed = itensDispensaGeral;
      if (itemDispensaMed.length) {
        const groupedMed = itemDispensaMed.filter(item => 
          String(item.dsc_item).toLowerCase().includes("medicamento") || 
          String(item.dsc_item).toLowerCase().includes("remedio")
        ).reduce((acc: any, item: any) => {
          const key = `${item.cod_orgao || 'S_ORG'}_${item.cod_item || 'S_COD'}_${item.num_ano_referencia || 'S_ANO'}`;
          if (!acc[key]) acc[key] = { total: 0, item };
          acc[key].total += parseNumber(item.vlr_empenhado);
          return acc;
        }, {});
        Object.entries(groupedMed).forEach(([key, data]: [string, any]) => {
          if (data.total > 50000) {
            addResult(
              "Fracionamento de Medicamentos", 
              data.item, 
              `Total acumulado de R$ ${data.total.toLocaleString('pt-BR', {minimumFractionDigits: 2})} em medicamentos via dispensa excede limites legais (Lei 14.133/21).`, 
              key, 
              data.total,
              "Realizar processo licitatório regular (Pregão Eletrônico) para a compra de medicamentos, considerando o planejamento anual de consumo da rede de saúde."
            );
          }
        });
      }

      // 42. Terceirização de Saúde (OS/OSCIP)
      const empenhosTerceirizados = empenhosGeral;
      const terceirizados = empenhosTerceirizados.filter(e => 
        String(e.cod_funcao) === "10" && 
        (String(e.nom_credor).toLowerCase().includes("instituto") || 
         String(e.nom_credor).toLowerCase().includes("associacao") ||
         String(e.nom_credor).toLowerCase().includes("fundacao"))
      );
      if (terceirizados.length) {
        const totalSaude = empenhosTerceirizados.filter(e => String(e.cod_funcao) === "10")
          .reduce((sum, e) => sum + parseNumber(e.vlr_empenho), 0);
        const totalTerceirizado = terceirizados.reduce((sum, e) => sum + parseNumber(e.vlr_empenho), 0);
        
        if (totalSaude > 0 && (totalTerceirizado / totalSaude) > 0.4) {
          addResult(
            "Alta Terceirização na Saúde",
            terceirizados[0],
            `Gastos com entidades do terceiro setor (OS/OSCIP) representam ${( (totalTerceirizado/totalSaude)*100 ).toFixed(2)}% do orçamento total da saúde.`,
            undefined,
            totalTerceirizado,
            "Avaliar a economicidade da gestão terceirizada em comparação com a gestão direta e garantir a fiscalização rigorosa das metas e resultados pactuados."
          );
        }
      }

      // 43. Consumo de Combustível em Ambulâncias
      const gastoFrotaSaude = gastoFrotaGeral;
      if (gastoFrotaSaude.length) {
        gastoFrotaSaude.filter(g => 
          String(g.dsc_tipo_veiculo).toLowerCase().includes("ambulancia") || 
          String(g.dsc_objeto).toLowerCase().includes("ambulancia")
        ).forEach(g => {
          const mFinal = parseNumber(g.num_marc_final);
          const mInitial = parseNumber(g.num_marc_inicial);
          const quant = parseNumber(g.num_quant_utilizada);
          if (quant > 0) {
            const km = (mFinal - mInitial) / quant;
            if (km < 4) { // Ambulâncias costumam ter consumo maior, mas < 4km/l é suspeito
              addResult(
                "Eficiência de Ambulância", 
                g, 
                `Consumo de ${km.toFixed(2)} km/l abaixo do esperado para a ambulância ${g.num_placa || 'N/A'}.`, 
                g.num_placa, 
                km.toFixed(2),
                "Verificar o estado mecânico da ambulância, a correção dos registros de quilometragem e a possível ocorrência de desvios de combustível."
              );
            }
          }
        });
      }

      // 44. Gastos com OPME (Órteses, Próteses e Materiais Especiais)
      const empenhosOPME = empenhosGeral;
      const opmeItems = empenhosOPME.filter(e => 
        String(e.cod_funcao) === "10" && 
        (String(e.dsc_objeto).toLowerCase().includes("protese") || 
         String(e.dsc_objeto).toLowerCase().includes("ortese") ||
         String(e.dsc_objeto).toLowerCase().includes("marcapasso"))
      );
      opmeItems.forEach(e => {
        const vlr = parseNumber(e.vlr_empenho);
        if (vlr > 20000) {
          addResult(
            "Gasto Elevado com OPME",
            e,
            `Aquisição de material especial (OPME) de alto valor individual (R$ ${vlr.toLocaleString('pt-BR', {minimumFractionDigits: 2})}).`,
            e.num_empenho,
            vlr,
            "Auditar a necessidade clínica do material, a regularidade da licitação e a compatibilidade do preço com a tabela SUS ou mercado."
          );
        }
      });

      // 45. Diárias Acumuladas (Pessoal)
      const empenhosDiarias = empenhosSaude.filter(e => 
        String(e.cod_elemento_despesa) === "339014" || // Diárias Civil
        String(e.dsc_objeto).toLowerCase().includes("diária") ||
        String(e.dsc_objeto).toLowerCase().includes("diaria")
      );
      if (empenhosDiarias.length) {
        const diariaMap = empenhosDiarias.reduce((acc: any, e: any) => {
          const key = sanitizeId(e.num_documento_credor);
          if (!acc[key]) acc[key] = { count: 0, total: 0, nom: e.nom_credor, item: e };
          acc[key].count += 1;
          acc[key].total += parseNumber(e.vlr_empenho);
          return acc;
        }, {});
        Object.entries(diariaMap).forEach(([cnpj, data]: [string, any]) => {
          if (data.count > 10) {
            addResult(
              "Diárias Acumuladas",
              data.item,
              `O beneficiário ${data.nom} recebeu ${data.count} diárias no período, totalizando R$ ${data.total.toLocaleString('pt-BR', {minimumFractionDigits: 2})}.`,
              cnpj,
              data.total,
              "Verificar a justificativa para o elevado número de viagens e se as prestações de contas foram devidamente aprovadas."
            );
          }
        });
      }

      // 46. Inconsistência de Carga Horária (Pessoal)
      const pessoalCH = pessoalGeral;
      if (pessoalCH.length) {
        const cpfCHMap = pessoalCH.reduce((acc: any, p: any) => {
          const cpf = sanitizeId(p.num_cpf);
          const orgName = orgaoMap[String(p.cod_orgao || "").trim()] || orgaoMap[String(p.seq_orgao || "").trim()] || p.nom_orgao || p.cod_orgao || "Órgão Não Identificado";
          if (!acc[cpf]) acc[cpf] = { totalCH: 0, nom: p.nom_servidor, item: p, orgaos: new Set() };
          acc[cpf].totalCH += parseInt(p.num_carga_horaria || 0, 10);
          acc[cpf].orgaos.add(`${orgName} (${p.num_carga_horaria}h)`);
          return acc;
        }, {});
        Object.entries(cpfCHMap).forEach(([cpf, data]: [string, any]) => {
          if (data.totalCH > 60) {
            const orgaosList = Array.from(data.orgaos).join(', ');
            addResult(
              "Inconsistência de Carga Horária",
              data.item,
              `O servidor ${data.nom} (CPF: ${cpf}) possui carga horária total de ${data.totalCH}h semanais, distribuída nos seguintes órgãos: ${orgaosList}. O limite legal é de 60h semanais para acúmulos permitidos.`,
              cpf,
              data.totalCH,
              "Instaurar processo administrativo para apurar o acúmulo de cargos e a compatibilidade de horários, visando a regularização da jornada."
            );
          }
        });
      }

      // 47. Divergência de Preço Unitário (Licitação)
      const itensLicitPreco = itensLicitGeral;
      if (itensLicitPreco.length) {
        const itemPrecoMap = itensLicitPreco.reduce((acc: any, i: any) => {
          const key = sanitizeId(i.cod_item);
          if (!acc[key]) acc[key] = [];
          acc[key].push({ vlr: parseNumber(i.vlr_unit_item), lic: i.num_licitacao, item: i });
          return acc;
        }, {});
        Object.entries(itemPrecoMap).forEach(([itemId, prices]: [string, any]) => {
          if (prices.length > 1) {
            const max = Math.max(...prices.map((p: any) => p.vlr));
            const min = Math.min(...prices.map((p: any) => p.vlr));
            if (min > 0 && (max / min) > 2) { // Preço dobrou
              const itemMax = prices.find((p: any) => p.vlr === max);
              addResult(
                "Divergência de Preço Unitário",
                itemMax.item,
                `O item ${itemId} foi licitado com variação de preço superior a 100% (Min: R$ ${min.toFixed(2)}, Max: R$ ${max.toFixed(2)}) entre diferentes certames.`,
                itemId,
                max,
                "Realizar pesquisa de preços de mercado rigorosa para identificar possíveis sobrepreços ou variações injustificadas nos valores de referência."
              );
            }
          }
        });
      }

      // 48. Outros Serviços PF Elevado (Despesa)
      const empenhosPF = empenhosGeral;
      const pfEmpenhos = empenhosPF.filter(e => 
        String(e.cod_elemento_despesa) === "339036" // Outros Serviços de Terceiros - PF
      );
      if (pfEmpenhos.length) {
        const pfMap = pfEmpenhos.reduce((acc: any, e: any) => {
          const key = sanitizeId(e.num_documento_credor);
          if (!acc[key]) acc[key] = { total: 0, nom: e.nom_credor, item: e };
          acc[key].total += parseNumber(e.vlr_empenho);
          return acc;
        }, {});
        Object.entries(pfMap).forEach(([cpf, data]: [string, any]) => {
          if (data.total > 10000) {
            addResult(
              "Serviços PF Elevados",
              data.item,
              `O credor PF ${data.nom} recebeu R$ ${data.total.toLocaleString('pt-BR', {minimumFractionDigits: 2})} em serviços de terceiros, valor que sugere a necessidade de contratação regular.`,
              cpf,
              data.total,
              "Avaliar se a prestação de serviço possui natureza contínua, o que exigiria a realização de concurso público ou contratação via CLT/Estatutário."
            );
          }
        });
      }

      // 49. Licitação com Vencedor Único em Vários Itens (Licitação)
      const homologacoes = homologacoesGeral;
      if (homologacoes.length) {
        const licVencMap = homologacoes.reduce((acc: any, h: any) => {
          const licId = sanitizeId(h.num_licitacao);
          const vencId = sanitizeId(h.num_documento_vencedor);
          if (!acc[licId]) acc[licId] = new Set();
          acc[licId].add(vencId);
          return acc;
        }, {});
        Object.entries(licVencMap).forEach(([licId, vencs]: [string, any]) => {
          if (vencs.size === 1) {
            addResult(
              "Vencedor Único em Licitação Multi-item",
              { num_licitacao: licId },
              `A licitação ${licId} teve todos os seus itens vencidos por um único fornecedor, o que pode indicar restrição à competitividade por lote.`,
              licId,
              undefined,
              "Verificar se o agrupamento de itens em lotes foi excessivo e se impediu a participação de fornecedores especializados em itens específicos."
            );
          }
        });
      }

      // 50. Despesa sem Empenho Prévio (Despesa)
      const pagamentosData = pagamentosDespesaGeral;
      const empenhosData = empenhosGeral;
      if (pagamentosData.length && empenhosData.length) {
        const empMap = empenhosData.reduce((acc: any, e: any) => {
          const key = sanitizeId(e.num_empenho);
          if (key) acc[key] = e;
          return acc;
        }, {});

        pagamentosData.forEach(p => {
          const emp = empMap[sanitizeId(p.num_empenho)];
          if (emp) {
            const dPag = parseDate(p.dat_pagamento);
            const dEmp = parseDate(emp.dat_empenho);
            if (dPag && dEmp && dPag < dEmp) {
              addResult(
                "Pagamento Antecipado ao Empenho",
                p,
                `O pagamento foi realizado em ${dPag.toLocaleDateString('pt-BR')}, data anterior ao empenho da despesa (${dEmp.toLocaleDateString('pt-BR')}).`,
                p.num_empenho,
                p.vlr_pagamento,
                "Respeitar os estágios da despesa pública (empenho, liquidação e pagamento), garantindo que nenhum pagamento seja efetuado sem o prévio empenho."
              );
            }
          }
        });
      }

      // 51. Conformidade de Horas Extras (Pessoal)
      const pessoalHE = pessoalGeral;
      if (pessoalHE.length) {
        pessoalHE.forEach(p => {
          const vlrBruto = parseNumber(p.vlr_bruto);
          const vlrExtra = parseNumber(p.vlr_horas_extras || 0);
          const ch = parseNumber(p.num_carga_horaria || 0);
          const nom = p.nom_servidor || "N/A";
          const cpf = sanitizeId(p.num_cpf);

          // Check 1: Overtime value > 30% of gross salary
          if (vlrBruto > 0 && vlrExtra > (vlrBruto * 0.3)) {
            addResult(
              "Conformidade de Horas Extras (Valor)",
              p,
              `Servidor ${nom} recebeu R$ ${vlrExtra.toLocaleString('pt-BR', {minimumFractionDigits: 2})} em horas extras, o que representa ${( (vlrExtra/vlrBruto)*100 ).toFixed(2)}% do salário bruto.`,
              cpf,
              vlrExtra,
              "Justificar a necessidade de horas extras em volume elevado e verificar se há compatibilidade com a jornada ordinária."
            );
          }

          // Check 2: Single contract workload > 44h (standard limit)
          if (ch > 44) {
            addResult(
              "Conformidade de Horas Extras (Jornada)",
              p,
              `Servidor ${nom} possui carga horária contratual de ${ch}h semanais em um único vínculo, excedendo o limite padrão de 44h.`,
              cpf,
              ch,
              "Verificar a legalidade da jornada de trabalho contratada e se há autorização para regime especial de trabalho."
            );
          }

          // Check 3: High workload + High overtime value (Fatigue Risk)
          if (ch >= 40 && vlrExtra > (vlrBruto * 0.2)) {
            addResult(
              "Conformidade de Horas Extras (Risco de Fadiga)",
              p,
              `Servidor ${nom} possui jornada de ${ch}h e recebeu R$ ${vlrExtra.toLocaleString('pt-BR', {minimumFractionDigits: 2})} em horas extras. O acúmulo sugere excesso de jornada física.`,
              cpf,
              vlrExtra,
              "Avaliar a saúde ocupacional do servidor e a eficiência da escala de trabalho, visando reduzir a dependência de horas extras."
            );
          }
        });
      }

      // 52. Contratações com Serviços Idênticos em Órgãos Diferentes
      const objectsByOrg: Record<string, Set<string>> = {};
      const objectDetails: Record<string, any> = {};

      const processObject = (item: any, description: string) => {
        if (!description || description.length < 20) return;
        
        // Normalização básica: minúsculas, remove acentos (opcional), remove pontuação e espaços extras
        const normalized = description.toLowerCase().trim()
          .normalize("NFD").replace(/[\u0300-\u036f]/g, "") // Remove acentos
          .replace(/[^\w\s]/gi, '')
          .replace(/\s+/g, ' ');
          
        if (normalized.length < 20) return;

        const codOrgao = String(item.cod_orgao || item.cod_unidade_gestora || "").trim();
        if (!codOrgao) return;

        if (!objectsByOrg[normalized]) {
          objectsByOrg[normalized] = new Set();
          objectDetails[normalized] = item;
        }
        objectsByOrg[normalized].add(codOrgao);
      };

      empenhosGeral.forEach(e => processObject(e, e.dsc_objeto));
      contratosGeral.forEach(c => processObject(c, c.dsc_objetocontrato));

      Object.entries(objectsByOrg).forEach(([normalized, orgs]) => {
        if (orgs.size > 1) {
          const item = objectDetails[normalized];
          const orgNames = Array.from(orgs).map(id => orgaoMap[id] || id).join(", ");
          const originalDesc = item.dsc_objeto || item.dsc_objetocontrato;
          
          addResult(
            "Contratações com Serviços Idênticos em Órgãos Diferentes",
            item,
            `O objeto "${originalDesc}" foi identificado em ${orgs.size} órgãos diferentes: ${orgNames}. Esta duplicidade sugere que a administração poderia se beneficiar de compras centralizadas ou Atas de Registro de Preços compartilhadas.`,
            undefined,
            undefined,
            "Recomenda-se que a Unidade Central de Compras avalie a consolidação destas demandas em processos licitatórios únicos ou registros de preços para ganho de escala e padronização."
          );
        }
      });

      res.json(results);
    } catch (error: any) {
      console.error("Erro ao processar auditoria:", error);
      res.status(500).json({ error: error.message || String(error) });
    }
  });

  app.get("/api/audit-trails", (req, res) => {
    const trails = [
      "Pagamentos em Duplicidade",
      "Fracionamento de Despesa",
      "Sobrepreço em Licitação",
      "Acúmulo de Cargos",
      "Nepotismo",
      "Despesas sem Empenho Prévio",
      "Receitas Não Arrecadadas",
      "Frota: Consumo Excessivo",
      "Saúde: Medicamentos Vencidos",
      "Educação: Merenda Escolar",
      "Contratos Prorrogados Indevidamente",
      "Dispensa de Licitação Irregular",
      "Aditivos Contratuais Acima do Limite",
      "Pagamentos a Empresas Inidôneas",
      "Servidores Fantasmas",
      "Desvio de Função",
      "Uso Indevido de Veículos Oficiais",
      "Irregularidades em Diárias",
      "Obras Paralisadas",
      "Inconsistências no Portal da Transparência",
      "Falta de Repasse Previdenciário",
      "Excesso de Cargos em Comissão",
      "Licitações com Cláusulas Restritivas",
      "Dispensa de Licitação por Emergência Fabricada",
      "Pagamento por Serviços Não Executados",
      "Superfaturamento de Insumos Médicos",
      "Irregularidades no Transporte Escolar",
      "Falta de Inventário de Bens Móveis",
      "Descumprimento da Lei de Responsabilidade Fiscal",
      "Inconsistências na Conciliação Bancária",
      "Pagamentos a Servidores Falecidos",
      "Acúmulo de Proventos e Vencimentos Acima do Teto",
      "Contratação de MEI para Serviços Contínuos",
      "Ausência de Pesquisa de Preços",
      "Direcionamento de Licitação",
      "Jogo de Planilha em Obras Públicas",
      "Pagamentos Realizados no Último Ano de Mandato",
      "Inexistência de Almoxarifado Central",
      "Falta de Controle de Combustível",
      "Irregularidades em Convênios",
      "Plantões Médicos Excessivos",
      "Abastecimento Suspeito",
      "Licitação com Participante Único",
      "Gasto com Publicidade",
      "Horas Extras Excessivas",
      "Renúncia de Receita",
      "Fracionamento de Medicamentos",
      "Alta Terceirização na Saúde",
      "Eficiência de Ambulância",
      "Gasto Elevado com OPME",
      "Diárias Acumuladas",
      "Inconsistência de Carga Horária"
    ];
    res.json(trails);
  });

  app.get("/api/metadata", (req, res) => {
    try {
      const orgaoData = readCSV("Órgão", "ORGAO");
      const mainOrgao = orgaoData.find(o => o.nom_orgao) || orgaoData[0];
      if (mainOrgao) {
        res.json({
          nom_orgao: mainOrgao.nom_orgao || mainOrgao.dsc_unidade || mainOrgao.nom_unidade_gestora || "Órgão Não Identificado",
          nom_municipio: mainOrgao.nom_municipio || "Município Não Identificado",
          num_anoexercicio: mainOrgao.num_anoexercicio || new Date().getFullYear(),
          versao_dicionario: "2.0"
        });
      } else {
        res.json({
          nom_orgao: "Órgão Não Identificado",
          nom_municipio: "Município Não Identificado",
          num_anoexercicio: new Date().getFullYear(),
          versao_dicionario: "2.0"
        });
      }
    } catch (error) {
      res.status(500).json({ error: "Erro ao carregar metadados do órgão." });
    }
  });

  if (process.env.NODE_ENV !== "production") {
    const vite = await createViteServer({ server: { middlewareMode: true }, appType: "spa" });
    app.use(vite.middlewares);
  } else {
    const distPath = path.join(process.cwd(), "dist");
    app.use(express.static(distPath));
    app.get("*", (req, res) => res.sendFile(path.join(distPath, "index.html")));
  }

  app.listen(PORT, "0.0.0.0", () => console.log(`Server running on http://localhost:${PORT}`));
}

startServer();
