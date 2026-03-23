import React, { useState, useEffect } from 'react';
import axios from 'axios';
import { 
  Database, 
  FileText, 
  RefreshCw, 
  CheckCircle2, 
  AlertCircle,
  FolderOpen,
  Table as TableIcon,
  LayoutDashboard,
  ShieldAlert,
  ShieldCheck,
  BrainCircuit,
  Download,
  Search,
  Filter,
  FileDown,
  ArrowRight,
  X
} from 'lucide-react';
import { motion, AnimatePresence } from 'motion/react';
import { clsx, type ClassValue } from 'clsx';
import { twMerge } from 'tailwind-merge';
import { GoogleGenAI } from "@google/genai";
import { jsPDF } from 'jspdf';
import autoTable from 'jspdf-autotable';
import ReactMarkdown from 'react-markdown';

function cn(...inputs: ClassValue[]) {
  return twMerge(clsx(inputs));
}

interface FolderStatus {
  folder: string;
  count: number;
  files: string[];
}

interface AuditTrail {
  trail: string;
  id?: string;
  value?: any;
  detail: string;
  resolution?: string;
  fullData?: string;
  cod_orgao?: string;
  nom_orgao?: string;
  seq_orgao?: string;
}

interface Metadata {
  nom_orgao: string;
  nom_municipio: string;
  num_anoexercicio: number;
  versao_dicionario: string;
}

export default function App() {
  const [statuses, setStatuses] = useState<FolderStatus[]>([]);
  const [loading, setLoading] = useState(false);
  const [processing, setProcessing] = useState(false);
  const [auditResults, setAuditResults] = useState<AuditTrail[]>([]);
  const [aiInsight, setAiInsight] = useState<string | null>(null);
  const [generatingAi, setGeneratingAi] = useState(false);
  const [aiProgress, setAiProgress] = useState<{ current: number; total: number; trail: string } | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [searchTerm, setSearchTerm] = useState('');
  const [activeTab, setActiveTab] = useState<'status' | 'audit'>('status');
  const [metadata, setMetadata] = useState<Metadata | null>(null);
  const [entities, setEntities] = useState<{cod_orgao: string, nom_orgao: string}[]>([]);
  const [selectedEntity, setSelectedEntity] = useState<string>('all');
  const [showClearConfirm, setShowClearConfirm] = useState(false);
  const [auditTrails, setAuditTrails] = useState<string[]>([]);
  const [showTrailsList, setShowTrailsList] = useState(false);

  const fetchStatus = async () => {
    setLoading(true);
    try {
      const response = await axios.get('/api/files');
      setStatuses(response.data);
      const metaResponse = await axios.get('/api/metadata');
      setMetadata(metaResponse.data);
      
      // Fetch audit trails list
      const trailsResponse = await axios.get('/api/audit-trails');
      setAuditTrails(trailsResponse.data);
    } catch (err) {
      setError('Erro ao carregar status dos arquivos.');
    } finally {
      setLoading(false);
    }
  };

  const clearTables = async () => {
    setLoading(true);
    try {
      await axios.post('/api/files/clear');
      setAuditResults([]);
      setAiInsight(null);
      setEntities([]);
      setShowClearConfirm(false);
      fetchStatus();
    } catch (err) {
      setError('Erro ao limpar tabelas.');
    } finally {
      setLoading(false);
    }
  };

  const runAudit = async () => {
    setProcessing(true);
    setError(null);
    setAiInsight(null);
    try {
      const response = await axios.post('/api/audit/process');
      setAuditResults(response.data);
      
      // Fetch entities after audit
      const entResponse = await axios.get('/api/entities');
      setEntities(entResponse.data);
      setSelectedEntity('all');
      
      setActiveTab('audit');
    } catch (err: any) {
      setError(err.response?.data?.error || 'Erro ao processar auditoria.');
    } finally {
      setProcessing(false);
    }
  };

  const generateAiInsights = async () => {
    if (filteredResults.length === 0) return;
    setGeneratingAi(true);
    setAiInsight("");
    setAiProgress(null);
    try {
      const ai = new GoogleGenAI({ apiKey: process.env.GEMINI_API_KEY || "" });
      const entityName = selectedEntity === 'all' ? 'Todos os Órgãos' : entities.find(e => e.cod_orgao === selectedEntity)?.nom_orgao || 'Órgão Selecionado';
      
      // Agrupar resultados por trilha para gerar parecer individual
      const resultsByTrail = filteredResults.reduce((acc: Record<string, AuditTrail[]>, curr) => {
        if (!acc[curr.trail]) acc[curr.trail] = [];
        acc[curr.trail].push(curr);
        return acc;
      }, {});
 
      const trailsFound = Object.keys(resultsByTrail);
      setAiProgress({ current: 0, total: trailsFound.length, trail: "" });
      
      let accumulatedInsight = "";

      // Gerar parecer para cada trilha encontrada
      for (let i = 0; i < trailsFound.length; i++) {
        const trail = trailsFound[i];
        setAiProgress({ current: i + 1, total: trailsFound.length, trail });
        
        const trailData = resultsByTrail[trail].slice(0, 10); // Aumentado para 10 amostras para melhor contexto
        const prompt = `Como Auditor Sênior do TCE-MG, analise os seguintes indícios de irregularidades encontrados no SICOM para o órgão "${entityName}" na trilha específica: "${trail}".
        
        IMPORTANTE: Forneça um PARECER TÉCNICO individualizado para esta trilha, contendo:
        1. Resumo técnico do risco encontrado nesta trilha específica.
        2. Impacto financeiro ou operacional potencial.
        3. Recomendação específica de correção baseada na legislação vigente (Cite leis como Lei 14.133/21, LRF, etc., se aplicável).

        Dados da trilha "${trail}" (Amostra de ${trailData.length} registros):
        ${JSON.stringify(trailData, null, 2)}
        
        Responda em Markdown, usando o título "## ${trail}". Seja formal e técnico.`;

        try {
          const response = await ai.models.generateContent({
            model: "gemini-3-flash-preview",
            contents: prompt
          });

          if (response.text) {
            accumulatedInsight += response.text + "\n\n---\n\n";
            setAiInsight(accumulatedInsight);
          }
        } catch (trailErr) {
          console.error(`Erro na trilha ${trail}:`, trailErr);
          accumulatedInsight += `## ${trail}\n\nErro ao gerar parecer para esta trilha.\n\n---\n\n`;
          setAiInsight(accumulatedInsight);
        }
      }

      if (!accumulatedInsight) {
        throw new Error("A IA não retornou nenhuma resposta válida.");
      }
    } catch (err: any) {
      console.error("Erro na IA:", err);
      setError('Erro ao gerar insights de IA: ' + (err.message || 'Erro desconhecido'));
    } finally {
      setGeneratingAi(false);
      setAiProgress(null);
    }
  };

  const formatValue = (val: any) => {
    if (val === null || val === undefined || val === '') return '-';
    if (typeof val === 'number') {
      return new Intl.NumberFormat('pt-BR', { 
        minimumFractionDigits: 2,
        maximumFractionDigits: 2
      }).format(val);
    }
    
    const sVal = String(val).trim();
    let n = 0;
    
    if (sVal.includes(",") && sVal.includes(".")) {
      n = parseFloat(sVal.replace(/\./g, "").replace(",", "."));
    } else if (sVal.includes(",")) {
      n = parseFloat(sVal.replace(",", "."));
    } else {
      n = parseFloat(sVal);
    }
    
    if (!isNaN(n) && sVal.match(/^-?[\d.,]+%?$/)) {
      const isPercent = sVal.includes('%');
      const formatted = new Intl.NumberFormat('pt-BR', { 
        minimumFractionDigits: 2,
        maximumFractionDigits: 2
      }).format(n);
      return isPercent ? `${formatted}%` : formatted;
    }
    return sVal;
  };

  const formatCurrency = (val: any) => {
    if (val === null || val === undefined || val === '') return 'R$ 0,00';
    let n = 0;
    if (typeof val === 'number') {
      n = val;
    } else {
      const s = String(val).replace(/[^\d,.-]/g, '').trim();
      if (s.includes(",") && s.includes(".")) {
        n = parseFloat(s.replace(/\./g, "").replace(",", "."));
      } else if (s.includes(",")) {
        n = parseFloat(s.replace(",", "."));
      } else {
        n = parseFloat(s);
      }
    }
    if (isNaN(n)) return String(val || "N/A");
    return new Intl.NumberFormat('pt-BR', { 
      style: 'currency', 
      currency: 'BRL',
      minimumFractionDigits: 2,
      maximumFractionDigits: 2
    }).format(n);
  };

  const exportar_relatorio_pdf = () => {
    if (!metadata) return;

    const doc = new jsPDF();
    const pageWidth = doc.internal.pageSize.getWidth();

    // 1. Cabeçalho Formal
    doc.setFont('helvetica', 'bold');
    doc.setFontSize(14);
    doc.text("RELATÓRIO TÉCNICO DE FISCALIZAÇÃO - DADOS ABERTOS TCEMG", pageWidth / 2, 20, { align: 'center' });
    
    doc.setFontSize(10);
    doc.setFont('helvetica', 'normal');
    
    // If an entity is selected, show it in the header
    const selectedEntityName = selectedEntity === 'all' 
      ? metadata.nom_orgao 
      : entities.find(e => e.cod_orgao === selectedEntity)?.nom_orgao || metadata.nom_orgao;

    doc.text(`Órgão: ${selectedEntityName}`, 20, 35);
    doc.text(`Município: ${metadata.nom_municipio}`, 20, 42);
    doc.text(`Ano de Exercício: ${metadata.num_anoexercicio}`, 20, 49);
    doc.text(`Versão do Dicionário: ${metadata.versao_dicionario}`, 20, 56);

    // 2. Introdução
    doc.setFont('helvetica', 'bold');
    doc.text("1. INTRODUÇÃO", 20, 70);
    doc.setFont('helvetica', 'normal');
    const introText = "O presente relatório técnico tem por objetivo apresentar os resultados da fiscalização automatizada realizada sobre os dados abertos do SICOM/TCEMG. A análise foi conduzida em estrita conformidade com as Instruções Normativas do Tribunal de Contas do Estado de Minas Gerais (TCEMG) e as diretrizes da Lei de Responsabilidade Fiscal (LRF). O processo utilizou algoritmos de auditoria contínua aplicados sobre os metadados disponibilizados no portal de Dados Abertos, visando identificar indícios de irregularidades e riscos à gestão pública.";
    const splitIntro = doc.splitTextToSize(introText, pageWidth - 40);
    doc.text(splitIntro, 20, 77);

    // 3. Quadro Resumo
    doc.setFont('helvetica', 'bold');
    doc.text("2. QUADRO RESUMO DE INCONSISTÊNCIAS", 20, 110);
    
    const summaryData = Object.entries(
      filteredResults.reduce((acc: any, curr) => {
        if (!acc[curr.trail]) acc[curr.trail] = { count: 0, total: 0 };
        acc[curr.trail].count += 1;
        
        let val = 0;
        if (typeof curr.value === 'number') {
          val = curr.value;
        } else if (typeof curr.value === 'string') {
          const s = curr.value.replace(/[^\d,.-]/g, '').trim();
          if (s.includes(",") && s.includes(".")) {
            val = parseFloat(s.replace(/\./g, "").replace(",", "."));
          } else if (s.includes(",")) {
            val = parseFloat(s.replace(",", "."));
          } else {
            val = parseFloat(s);
          }
          if (isNaN(val)) val = 0;
        }
        
        acc[curr.trail].total += val;
        return acc;
      }, {})
    ).map(([trail, stats]: [string, any]) => [
      trail,
      stats.count,
      stats.total > 0 ? formatCurrency(stats.total) : "N/A"
    ]);

    autoTable(doc, {
      startY: 115,
      head: [['Trilha de Auditoria', 'Inconsistências', 'Valor sob Risco']],
      body: summaryData,
      theme: 'grid',
      headStyles: { fillColor: [180, 0, 0], textColor: [255, 255, 255] },
      styles: { fontSize: 8 }
    });

    // 4. Detalhamento (Nova Página se necessário)
    let finalY = (doc as any).lastAutoTable.finalY + 15;
    doc.setFont('helvetica', 'bold');
    doc.text("3. DETALHAMENTO DAS IRREGULARIDADES", 20, finalY);
    doc.setFont('helvetica', 'normal');
    finalY += 7;

    // Group by Organ
    const resultsByOrgan = filteredResults.reduce((acc: any, curr) => {
      const organ = curr.nom_orgao || 'Órgão Não Identificado';
      if (!acc[organ]) acc[organ] = [];
      acc[organ].push(curr);
      return acc;
    }, {});

    Object.entries(resultsByOrgan).forEach(([organ, items]: [string, any]) => {
      if (finalY > 250) {
        doc.addPage();
        finalY = 20;
      }
      
      const seqOrgao = items[0]?.seq_orgao || 'N/A';
      doc.setFontSize(11);
      doc.setFont('helvetica', 'bold');
      doc.setTextColor(180, 0, 0);
      doc.text(`ÓRGÃO: ${organ} (Seq: ${seqOrgao})`, 20, finalY);
      doc.setTextColor(0, 0, 0);
      finalY += 10;

      items.forEach((res: any, index: number) => {
        if (finalY > 250) {
          doc.addPage();
          finalY = 20;
        }
        doc.setFontSize(9);
        doc.setFont('helvetica', 'bold');
        doc.text(`${index + 1}. Trilha: ${res.trail}`, 20, finalY);
        doc.setFont('helvetica', 'normal');
        doc.text(`Identificador: ${res.id || 'N/A'} | Valor: ${formatValue(res.value)}`, 25, finalY + 5);
        
        const detailSplit = doc.splitTextToSize(`Indício: ${res.detail}`, pageWidth - 50);
        doc.text(detailSplit, 25, finalY + 10);
        finalY += (detailSplit.length * 4) + 10;

        doc.setFont('helvetica', 'bold');
        doc.text("Possível Forma de Resolução:", 25, finalY);
        doc.setFont('helvetica', 'normal');
        const resSplit = doc.splitTextToSize(res.resolution || "Verificar conformidade legal.", pageWidth - 50);
        doc.text(resSplit, 25, finalY + 5);
        finalY += (resSplit.length * 4) + 10;

        doc.setFont('helvetica', 'italic');
        doc.setFontSize(7);
        const dataSplit = doc.splitTextToSize(`Detalhes Completos do Item:\n${res.fullData || 'N/A'}`, pageWidth - 50);
        doc.text(dataSplit, 25, finalY);
        finalY += (dataSplit.length * 3) + 10;
      });
      
      finalY += 5; // Space between organs
    });

    // 5. Análise Qualitativa (AI Insights)
    if (aiInsight) {
      doc.addPage();
      finalY = 20;
      
      doc.setFontSize(12);
      doc.setFont('helvetica', 'bold');
      doc.setTextColor(180, 0, 0);
      doc.text("4. ANÁLISE QUALITATIVA E PARECER TÉCNICO (IA)", 20, finalY);
      doc.setTextColor(0, 0, 0);
      finalY += 10;

      // Strip Markdown for PDF
      const cleanAiInsight = aiInsight
        .replace(/#{1,6}\s/g, '') // Remove headers
        .replace(/\*\*(.*?)\*\*/g, '$1') // Remove bold
        .replace(/\*(.*?)\*/g, '$1') // Remove italic
        .replace(/\[(.*?)\]\(.*?\)/g, '$1') // Remove links
        .replace(/`{1,3}.*?`{1,3}/gs, '') // Remove code blocks
        .trim();

      const aiSplit = doc.splitTextToSize(cleanAiInsight, pageWidth - 40);
      doc.setFont('helvetica', 'normal');
      doc.setFontSize(9);
      
      aiSplit.forEach((line: string) => {
        if (finalY > 270) {
          doc.addPage();
          finalY = 20;
        }
        doc.text(line, 20, finalY);
        finalY += 5;
      });
      
      finalY += 10;
    }

    // 6. Fechamento
    if (finalY > 250) {
      doc.addPage();
      finalY = 20;
    }
    const dateStr = new Date().toLocaleDateString('pt-BR');
    doc.setFont('helvetica', 'normal');
    doc.setFontSize(10);
    doc.text(`Emitido em: ${dateStr}`, 20, finalY + 10);
    doc.text("Relatório gerado automaticamente pelo AuditFlow AI", pageWidth / 2, finalY + 25, { align: 'center' });
    
    doc.save(`Oficio_Auditoria_${metadata.nom_municipio}_${metadata.num_anoexercicio}.pdf`);
  };

  const exportCSV = () => {
    const headers = "Sequencial Órgão;Nome do Órgão;Trilha Identificada;Identificador;Valor;Indício;Possível Resolução;Detalhes Completos\n";
    const escapeCSV = (val: any) => {
      const s = String(val || '').replace(/"/g, '""');
      return `"${s}"`;
    };
    const rows = filteredResults.map(r => 
      `${escapeCSV(r.seq_orgao)};${escapeCSV(r.nom_orgao)};${escapeCSV(r.trail)};${escapeCSV(r.id)};${escapeCSV(formatValue(r.value))};${escapeCSV(r.detail)};${escapeCSV(r.resolution)};${escapeCSV(r.fullData)}`
    ).join("\n");
    // Add BOM for Excel compatibility
    const BOM = "\uFEFF";
    const blob = new Blob([BOM + headers + rows], { type: 'text/csv;charset=utf-8;' });
    const link = document.createElement("a");
    link.href = URL.createObjectURL(blob);
    link.setAttribute("download", "relatorio_auditoria_tcemg.csv");
    document.body.appendChild(link);
    link.click();
    document.body.removeChild(link);
  };

  const [uploading, setUploading] = useState(false);
  const [selectedFolder, setSelectedFolder] = useState<string>('Órgão');
  const [dragActive, setDragActive] = useState(false);

  const handleUpload = async (files: FileList | File[]) => {
    if (!files || files.length === 0) return;
    setUploading(true);
    setError(null);

    const fileArray = Array.from(files);
    let hasError = false;

    for (const file of fileArray) {
      const formData = new FormData();
      formData.append('folder', selectedFolder);
      formData.append('file', file);

      try {
        await axios.post('/api/upload', formData, {
          headers: { 'Content-Type': 'multipart/form-data' }
        });
      } catch (err: any) {
        const msg = err.response?.data?.error || err.message || `Erro ao carregar ${file.name}`;
        setError(`Erro ao carregar ${file.name}: ${msg}`);
        hasError = true;
        break; // Stop on first error
      }
    }

    if (!hasError) {
      fetchStatus();
    }
    setUploading(false);
  };

  const onDragOver = (e: React.DragEvent) => {
    e.preventDefault();
    setDragActive(true);
  };

  const onDragLeave = () => setDragActive(false);

  const onDrop = (e: React.DragEvent) => {
    e.preventDefault();
    setDragActive(false);
    if (e.dataTransfer.files && e.dataTransfer.files.length > 0) {
      handleUpload(e.dataTransfer.files);
    }
  };

  const onFileChange = (e: React.ChangeEvent<HTMLInputElement>) => {
    if (e.target.files && e.target.files.length > 0) {
      handleUpload(e.target.files);
    }
  };

  useEffect(() => {
    fetchStatus();
  }, []);

  const filteredResults = auditResults.filter(r => {
    const searchLower = searchTerm.toLowerCase();
    const matchesSearch = 
      r.trail.toLowerCase().includes(searchLower) || 
      r.detail.toLowerCase().includes(searchLower) ||
      (r.id && String(r.id).toLowerCase().includes(searchLower)) ||
      (r.nom_orgao && r.nom_orgao.toLowerCase().includes(searchLower)) ||
      (r.value !== undefined && String(r.value).toLowerCase().includes(searchLower));
      
    const matchesEntity = selectedEntity === 'all' || r.cod_orgao === selectedEntity;
    return matchesSearch && matchesEntity;
  });

  const [expandedTrails, setExpandedTrails] = useState<Record<string, boolean>>({});

  const toggleTrail = (trail: string) => {
    setExpandedTrails(prev => ({
      ...prev,
      [trail]: !prev[trail]
    }));
  };

  const groupedResults = filteredResults.reduce((acc: Record<string, AuditTrail[]>, curr) => {
    if (!acc[curr.trail]) acc[curr.trail] = [];
    acc[curr.trail].push(curr);
    return acc;
  }, {});

  return (
    <div className="min-h-screen bg-[#F8FAFC] text-slate-900 font-sans selection:bg-indigo-100">
      {/* Header */}
      <header className="bg-white border-b border-slate-200 sticky top-0 z-10">
        <div className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8 h-16 flex items-center justify-between">
          <div className="flex items-center gap-3">
            <div className="bg-rose-600 p-2 rounded-lg">
              <ShieldAlert className="w-6 h-6 text-white" />
            </div>
            <div>
              <h1 className="text-xl font-bold tracking-tight text-slate-900">Auditor Digital TCEMG</h1>
              <p className="text-[10px] text-slate-500 font-bold uppercase tracking-widest">Inteligência em Fiscalização Governamental</p>
            </div>
          </div>
          <div className="flex items-center gap-4">
            <nav className="flex bg-slate-100 p-1 rounded-xl mr-4">
              <button 
                onClick={() => setActiveTab('status')}
                className={cn("px-4 py-1.5 text-xs font-bold rounded-lg transition-all", activeTab === 'status' ? "bg-white text-slate-900 shadow-sm" : "text-slate-500 hover:text-slate-700")}
              >
                Arquivos
              </button>
              <button 
                onClick={() => setActiveTab('audit')}
                className={cn("px-4 py-1.5 text-xs font-bold rounded-lg transition-all", activeTab === 'audit' ? "bg-white text-slate-900 shadow-sm" : "text-slate-500 hover:text-slate-700")}
              >
                Auditoria
              </button>
            </nav>
            <button 
              onClick={fetchStatus}
              disabled={loading}
              className="p-2 text-slate-500 hover:text-indigo-600 hover:bg-indigo-50 rounded-full transition-all"
            >
              <RefreshCw className={cn("w-5 h-5", loading && "animate-spin")} />
            </button>
          </div>
        </div>
      </header>

      <main className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8 py-8">
        {error && (
          <motion.div 
            initial={{ opacity: 0, y: -20 }}
            animate={{ opacity: 1, y: 0 }}
            className="mb-6 p-4 bg-rose-50 border border-rose-200 rounded-2xl flex items-center gap-3 text-rose-700 text-sm font-medium"
          >
            <AlertCircle className="w-5 h-5 flex-shrink-0" />
            <p>{error}</p>
            <button onClick={() => setError(null)} className="ml-auto text-rose-400 hover:text-rose-600">
              <RefreshCw className="w-4 h-4" />
            </button>
          </motion.div>
        )}
        <div className="grid grid-cols-1 lg:grid-cols-12 gap-8">
          
          {/* Sidebar */}
          <div className="lg:col-span-4 space-y-6">
            <section className="bg-white rounded-2xl border border-slate-200 shadow-sm overflow-hidden">
              <div className="p-5 border-b border-slate-100 flex items-center justify-between bg-slate-50/50">
                <div className="flex flex-col">
                  <div className="flex items-center gap-2">
                    <FolderOpen className="w-5 h-5 text-slate-400" />
                    <h2 className="font-semibold text-slate-800">Status do Dicionário</h2>
                  </div>
                  {metadata && (
                    <div className="mt-1 flex items-center gap-2 text-[10px] font-bold text-slate-500 uppercase tracking-wider">
                      <span className="bg-slate-200/50 px-1.5 py-0.5 rounded text-slate-600">{metadata.nom_municipio}</span>
                      <span className="bg-slate-200/50 px-1.5 py-0.5 rounded text-slate-600">{metadata.num_anoexercicio}</span>
                    </div>
                  )}
                </div>
                <button 
                  onClick={() => setShowClearConfirm(true)}
                  className="px-3 py-1.5 bg-rose-50 hover:bg-rose-100 text-rose-600 rounded-lg text-[10px] font-bold uppercase tracking-widest flex items-center gap-1.5 transition-colors border border-rose-200/50"
                >
                  <RefreshCw className="w-3 h-3" />
                  Limpar Tabelas
                </button>
              </div>
              {showClearConfirm && (
                <div className="p-4 bg-rose-50 border-b border-rose-100">
                  <p className="text-xs font-bold text-rose-700 mb-2">Tem certeza que deseja limpar todas as tabelas?</p>
                  <div className="flex gap-2">
                    <button 
                      onClick={clearTables}
                      className="px-3 py-1 bg-rose-600 text-white text-[10px] font-bold rounded-lg"
                    >
                      Sim, Limpar
                    </button>
                    <button 
                      onClick={() => setShowClearConfirm(false)}
                      className="px-3 py-1 bg-white border border-slate-200 text-slate-600 text-[10px] font-bold rounded-lg"
                    >
                      Cancelar
                    </button>
                  </div>
                </div>
              )}
              <div className="divide-y divide-slate-100 max-h-[300px] overflow-y-auto">
                {statuses.map((s) => (
                  <div key={s.folder} className="p-4 flex items-center justify-between hover:bg-slate-50 transition-colors group">
                    <div className="flex items-center gap-3">
                      <div className={cn(
                        "w-8 h-8 rounded-lg flex items-center justify-center transition-colors",
                        s.count > 0 ? "bg-emerald-50 text-emerald-600" : "bg-slate-100 text-slate-400"
                      )}>
                        <FileText className="w-4 h-4" />
                      </div>
                      <p className="text-xs font-bold text-slate-600 group-hover:text-indigo-600 transition-colors uppercase tracking-tight">{s.folder}</p>
                    </div>
                    <span className="text-[10px] font-black text-slate-400">{s.count} CSV</span>
                  </div>
                ))}
              </div>
            </section>

            {/* Lista de Trilhas Analisadas */}
            <section className="bg-white rounded-2xl border border-slate-200 shadow-sm overflow-hidden">
              <button 
                onClick={() => setShowTrailsList(!showTrailsList)}
                className="w-full p-5 border-b border-slate-100 flex items-center justify-between bg-slate-50/50 hover:bg-slate-100 transition-colors"
              >
                <div className="flex items-center gap-2">
                  <ShieldCheck className="w-5 h-5 text-indigo-500" />
                  <h2 className="font-semibold text-slate-800">Trilhas Analisadas ({auditTrails.length})</h2>
                </div>
                <ArrowRight className={cn("w-4 h-4 text-slate-400 transition-transform", showTrailsList && "rotate-90")} />
              </button>
              {showTrailsList && (
                <div className="p-4 bg-slate-50/30 max-h-[400px] overflow-y-auto">
                  <div className="grid grid-cols-1 gap-2">
                    {auditTrails.map((trail, idx) => (
                      <div key={idx} className="flex items-start gap-2 text-[10px] font-medium text-slate-600 bg-white p-2 rounded-lg border border-slate-100">
                        <span className="bg-slate-100 text-slate-400 w-5 h-5 rounded flex items-center justify-center flex-shrink-0">{idx + 1}</span>
                        <span>{trail}</span>
                      </div>
                    ))}
                  </div>
                </div>
              )}
            </section>

            <div className="space-y-3">
              <button
                onClick={runAudit}
                disabled={processing}
                className={cn(
                  "w-full py-4 rounded-2xl font-bold text-white shadow-lg flex items-center justify-center gap-3 transition-all active:scale-[0.98]",
                  processing ? "bg-slate-400 cursor-not-allowed" : "bg-rose-600 hover:bg-rose-700 shadow-rose-200"
                )}
              >
                {processing ? <RefreshCw className="w-5 h-5 animate-spin" /> : <ShieldAlert className="w-5 h-5" />}
                Executar Trilhas de Auditoria
              </button>

              {auditResults.length > 0 && (
                <>
                  <button
                    onClick={generateAiInsights}
                    disabled={generatingAi}
                    className="w-full py-4 rounded-2xl font-bold text-indigo-600 border-2 border-indigo-600 hover:bg-indigo-50 flex flex-col items-center justify-center gap-1 transition-all"
                    title={selectedEntity === 'all' ? "Gerar insights para todos os órgãos" : `Gerar insights para ${selectedEntity}`}
                  >
                    <div className="flex items-center gap-3">
                      {generatingAi ? <RefreshCw className="w-5 h-5 animate-spin" /> : <BrainCircuit className="w-5 h-5" />}
                      <span>Gerar Insights com IA</span>
                    </div>
                    {selectedEntity !== 'all' && (
                      <span className="text-[10px] font-black uppercase tracking-widest opacity-60">
                        Filtro: {selectedEntity}
                      </span>
                    )}
                  </button>
                  <button
                    onClick={exportar_relatorio_pdf}
                    className="w-full py-4 rounded-2xl font-bold text-white bg-indigo-600 hover:bg-indigo-700 shadow-lg shadow-indigo-100 flex flex-col items-center justify-center gap-1 transition-all"
                    title={selectedEntity === 'all' ? "Exportar relatório de todos os órgãos" : `Exportar relatório de ${selectedEntity}`}
                  >
                    <div className="flex items-center gap-3">
                      <FileDown className="w-5 h-5" />
                      <span>Baixar Ofício de Auditoria (PDF)</span>
                    </div>
                    {selectedEntity !== 'all' && (
                      <span className="text-[10px] font-black uppercase tracking-widest opacity-40">
                        Filtro: {selectedEntity}
                      </span>
                    )}
                  </button>
                  <button
                    onClick={exportCSV}
                    className="w-full py-3 rounded-2xl font-bold text-slate-600 hover:bg-slate-100 flex items-center justify-center gap-3 transition-all"
                  >
                    <Download className="w-4 h-4" />
                    Exportar Planilha (CSV)
                  </button>
                </>
              )}
            </div>
          </div>

          {/* Main Content */}
          <div className="lg:col-span-8 space-y-6">
            <AnimatePresence mode="wait">
              {activeTab === 'status' ? (
                <motion.div 
                  key="status-view"
                  initial={{ opacity: 0, x: 20 }}
                  animate={{ opacity: 1, x: 0 }}
                  exit={{ opacity: 0, x: -20 }}
                  className="space-y-6"
                >
                  {/* Upload Section */}
                  <div className="bg-white rounded-3xl border border-slate-200 p-8 shadow-sm">
                    {metadata && (
                      <div className="mb-6 p-4 bg-indigo-50/50 rounded-2xl border border-indigo-100 flex items-center justify-between">
                        <div className="flex items-center gap-4">
                          <div className="flex flex-col">
                            <span className="text-[10px] font-black text-indigo-400 uppercase tracking-widest">Município Analisado</span>
                            <span className="text-sm font-bold text-indigo-900">{metadata.nom_municipio}</span>
                          </div>
                          <div className="w-px h-8 bg-indigo-200" />
                          <div className="flex flex-col">
                            <span className="text-[10px] font-black text-indigo-400 uppercase tracking-widest">Ano de Exercício</span>
                            <span className="text-sm font-bold text-indigo-900">{metadata.num_anoexercicio}</span>
                          </div>
                        </div>
                        <div className="text-right">
                          <span className="text-[10px] font-black text-indigo-400 uppercase tracking-widest">Versão SICOM</span>
                          <p className="text-xs font-bold text-indigo-900">{metadata.versao_dicionario}</p>
                        </div>
                      </div>
                    )}
                    <div className="flex items-center gap-3 mb-6">
                      <div className="bg-indigo-50 p-2 rounded-lg">
                        <FileDown className="w-5 h-5 text-indigo-600" />
                      </div>
                      <div>
                        <h3 className="text-lg font-bold text-slate-800">Upload de Arquivos SICOM</h3>
                        <p className="text-xs text-slate-500">Selecione a categoria e arraste seu arquivo CSV</p>
                      </div>
                    </div>

                    <div className="grid grid-cols-1 md:grid-cols-3 gap-6">
                      <div className="space-y-2">
                        <label className="text-[10px] font-black text-slate-400 uppercase tracking-widest">Categoria TCEMG</label>
                        <select 
                          value={selectedFolder}
                          onChange={(e) => setSelectedFolder(e.target.value)}
                          className="w-full p-3 bg-slate-50 border border-slate-200 rounded-xl text-sm focus:outline-none focus:ring-2 focus:ring-indigo-500 appearance-none cursor-pointer"
                        >
                          {statuses.map(s => (
                            <option key={s.folder} value={s.folder}>{s.folder}</option>
                          ))}
                        </select>
                      </div>

                      <div className="md:col-span-2">
                        <div 
                          onDragOver={onDragOver}
                          onDragLeave={onDragLeave}
                          onDrop={onDrop}
                          className={cn(
                            "relative border-2 border-dashed rounded-2xl p-8 flex flex-col items-center justify-center transition-all cursor-pointer",
                            dragActive ? "border-indigo-500 bg-indigo-50/50" : "border-slate-200 hover:border-indigo-400 hover:bg-slate-50",
                            uploading && "opacity-50 cursor-not-allowed"
                          )}
                          onClick={() => !uploading && document.getElementById('file-upload')?.click()}
                        >
                          <input 
                            id="file-upload"
                            type="file" 
                            accept=".csv"
                            multiple
                            className="hidden"
                            onChange={onFileChange}
                            disabled={uploading}
                          />
                          {uploading ? (
                            <RefreshCw className="w-8 h-8 text-indigo-500 animate-spin mb-2" />
                          ) : (
                            <Download className="w-8 h-8 text-slate-300 mb-2" />
                          )}
                          <p className="text-sm font-bold text-slate-600">
                            {uploading ? "Fazendo upload de múltiplos arquivos..." : "Arraste os arquivos ou clique para selecionar"}
                          </p>
                          <p className="text-[10px] text-slate-400 mt-1">Você pode selecionar vários arquivos .csv de uma vez</p>
                        </div>
                      </div>
                    </div>
                  </div>

                  {/* Empty State / Info */}
                  <div className="bg-slate-50 rounded-3xl border-2 border-dashed border-slate-200 h-[300px] flex flex-col items-center justify-center text-center p-8">
                    <div className="bg-white p-4 rounded-full mb-4 shadow-sm">
                      <Database className="w-8 h-8 text-slate-300" />
                    </div>
                    <h3 className="text-lg font-bold text-slate-800 mb-1">Repositório de Dados</h3>
                    <p className="text-slate-500 max-w-md mx-auto text-xs">
                      Os arquivos carregados serão processados automaticamente pelo motor de auditoria. Use o painel lateral para acompanhar o status de cada diretório.
                    </p>
                  </div>
                </motion.div>
              ) : (
                <motion.div 
                  key="audit-view"
                  initial={{ opacity: 0, x: 20 }}
                  animate={{ opacity: 1, x: 0 }}
                  exit={{ opacity: 0, x: -20 }}
                  className="space-y-6"
                >
                  {/* Dashboard Summary - Removed as requested */}
                  <div className="grid grid-cols-1 md:grid-cols-3 gap-4">
                    <div className="bg-white p-6 rounded-3xl border border-slate-200 shadow-sm">
                      <p className="text-[10px] font-black text-slate-400 uppercase tracking-widest mb-1">Total de Indícios</p>
                      <h3 className="text-3xl font-bold text-rose-600">{filteredResults.length}</h3>
                      <p className="text-[10px] text-slate-500 mt-2 font-medium">Irregularidades detectadas no período</p>
                    </div>
                    <div className="bg-white p-6 rounded-3xl border border-slate-200 shadow-sm">
                      <p className="text-[10px] font-black text-slate-400 uppercase tracking-widest mb-1">Trilhas Ativas</p>
                      <div className="flex items-center justify-between">
                        <h3 className="text-3xl font-bold text-indigo-600">52</h3>
                        <button 
                          onClick={() => setShowTrailsList(true)}
                          className="text-[10px] font-bold text-indigo-600 hover:underline uppercase tracking-widest"
                        >
                          Ver Lista
                        </button>
                      </div>
                      <p className="text-[10px] text-slate-500 mt-2 font-medium">Regras de negócio automatizadas</p>
                    </div>
                    <div className="bg-white p-6 rounded-3xl border border-slate-200 shadow-sm">
                      <p className="text-[10px] font-black text-slate-400 uppercase tracking-widest mb-1">Categorias</p>
                      <h3 className="text-3xl font-bold text-emerald-600">9</h3>
                      <p className="text-[10px] text-slate-500 mt-2 font-medium">Áreas de fiscalização cobertas</p>
                    </div>
                  </div>

                  {/* AI Progress Indicator */}
                  {generatingAi && (
                    <div className="bg-indigo-50 border border-indigo-100 p-6 rounded-3xl mb-6 flex items-center gap-4 animate-pulse">
                      <RefreshCw className="w-6 h-6 text-indigo-600 animate-spin" />
                      <div>
                        <h3 className="text-sm font-bold text-indigo-900 uppercase tracking-wider">Gerando Parecer Técnico IA...</h3>
                        <p className="text-xs text-indigo-600 font-medium mt-1">
                          Processando trilha {aiProgress.current} de {aiProgress.total}: <span className="font-black">{aiProgress.trail}</span>
                        </p>
                      </div>
                    </div>
                  )}

                  {/* AI Insight Card */}
                  {aiInsight && (
                    <div className="bg-indigo-600 text-white p-8 rounded-3xl shadow-xl shadow-indigo-200 relative overflow-hidden">
                      <div className="absolute top-0 right-0 p-4 opacity-5">
                        <BrainCircuit className="w-48 h-48" />
                      </div>
                      <div className="flex items-center gap-3 mb-6">
                        <div className="bg-white/20 p-2 rounded-xl">
                          <BrainCircuit className="w-6 h-6" />
                        </div>
                        <h3 className="font-black uppercase tracking-[0.2em] text-sm">Parecer Técnico IA (Gemini)</h3>
                      </div>
                      <div className="prose prose-invert prose-sm max-w-none">
                        <ReactMarkdown 
                          components={{
                            p: ({children}) => <p className="mb-4 text-indigo-50 leading-relaxed font-medium">{children}</p>,
                            h1: ({children}) => <h1 className="text-xl font-bold mb-4 text-white border-b border-white/20 pb-2">{children}</h1>,
                            h2: ({children}) => <h2 className="text-lg font-bold mt-6 mb-3 text-white flex items-center gap-2">
                              <ArrowRight className="w-4 h-4" /> {children}
                            </h2>,
                            li: ({children}) => <li className="mb-2 text-indigo-100 list-disc ml-4">{children}</li>,
                            strong: ({children}) => <strong className="text-white font-black">{children}</strong>
                          }}
                        >
                          {aiInsight}
                        </ReactMarkdown>
                      </div>
                    </div>
                  )}

                  {/* Results Table */}
                  <div className="bg-white rounded-2xl border border-slate-200 shadow-sm overflow-hidden">
                    <div className="p-5 border-b border-slate-100 flex flex-col sm:flex-row sm:items-center justify-between gap-4 bg-slate-50/50">
                      <div className="flex items-center gap-2">
                        <ShieldAlert className="w-5 h-5 text-rose-500" />
                        <h2 className="font-bold text-slate-800">Indícios de Irregularidades ({filteredResults.length})</h2>
                      </div>
                      <div className="flex items-center gap-4">
                        {entities.length > 0 && (
                          <div className="relative flex items-center gap-2">
                            <Filter className="w-4 h-4 text-slate-400" />
                            <select 
                              value={selectedEntity}
                              onChange={(e) => setSelectedEntity(e.target.value)}
                              className="pl-3 pr-8 py-2 bg-white border border-slate-200 rounded-xl text-xs focus:outline-none focus:ring-2 focus:ring-indigo-500 appearance-none cursor-pointer font-bold text-slate-700"
                            >
                              <option value="all">Todas as Entidades (Geral)</option>
                              {entities.map(ent => (
                                <option key={ent.cod_orgao} value={ent.cod_orgao}>{ent.nom_orgao}</option>
                              ))}
                            </select>
                          </div>
                        )}
                        <div className="relative">
                          <Search className="w-4 h-4 absolute left-3 top-1/2 -translate-y-1/2 text-slate-400" />
                          <input 
                            type="text" 
                            placeholder="Filtrar trilha ou detalhe..."
                            value={searchTerm}
                            onChange={(e) => setSearchTerm(e.target.value)}
                            className="pl-9 pr-4 py-2 bg-white border border-slate-200 rounded-xl text-xs focus:outline-none focus:ring-2 focus:ring-indigo-500 w-full sm:w-64"
                          />
                        </div>
                      </div>
                    </div>
                    <div className="overflow-x-auto">
                      <table className="w-full text-left border-collapse">
                        <thead>
                          <tr className="bg-slate-50 text-slate-500 text-[10px] uppercase tracking-widest font-black">
                            <th className="px-6 py-4 border-b border-slate-100">Órgão</th>
                            <th className="px-6 py-4 border-b border-slate-100">Identificador</th>
                            <th className="px-6 py-4 border-b border-slate-100">Valor/Ref</th>
                            <th className="px-6 py-4 border-b border-slate-100">Detalhamento / Resolução</th>
                          </tr>
                        </thead>
                        <tbody className="divide-y divide-slate-50">
                          {Object.keys(groupedResults).length > 0 ? (Object.entries(groupedResults) as [string, AuditTrail[]][]).map(([trail, items], i) => (
                            <React.Fragment key={trail}>
                              <tr className="bg-slate-50/80 border-y border-slate-100 group cursor-pointer hover:bg-slate-100 transition-colors" onClick={() => toggleTrail(trail)}>
                                <td colSpan={4} className="px-6 py-3">
                                  <div className="flex items-center justify-between">
                                    <div className="flex items-center gap-3">
                                      <div className={cn(
                                        "w-5 h-5 rounded-md flex items-center justify-center transition-transform",
                                        expandedTrails[trail] ? "rotate-90" : "rotate-0"
                                      )}>
                                        <RefreshCw className="w-3 h-3 text-slate-400" />
                                      </div>
                                      <span className="text-xs font-black text-slate-700 uppercase tracking-tight">
                                        {trail}
                                      </span>
                                    </div>
                                    <div className="flex items-center gap-4">
                                      <span className="inline-flex items-center px-2 py-0.5 rounded-full text-[10px] font-bold bg-rose-100 text-rose-700">
                                        {items.length} {items.length === 1 ? 'indício' : 'indícios'}
                                      </span>
                                      <button 
                                        className="text-[10px] font-bold text-indigo-600 hover:text-indigo-800 uppercase tracking-widest"
                                      >
                                        {expandedTrails[trail] ? 'Recolher' : 'Ver Detalhes'}
                                      </button>
                                    </div>
                                  </div>
                                </td>
                              </tr>
                              <AnimatePresence>
                                {expandedTrails[trail] && items.map((res, idx) => (
                                  <motion.tr 
                                    key={`${trail}-${idx}`}
                                    initial={{ opacity: 0, height: 0 }}
                                    animate={{ opacity: 1, height: 'auto' }}
                                    exit={{ opacity: 0, height: 0 }}
                                    className="hover:bg-rose-50/30 transition-colors group border-b border-slate-50"
                                  >
                                    <td className="px-6 py-4 text-xs font-bold text-slate-700 align-top">
                                      <div className="flex flex-col">
                                        <span className="text-[10px] text-slate-400 font-mono">#{res.seq_orgao || '-'}</span>
                                        <span>{res.nom_orgao || '-'}</span>
                                      </div>
                                    </td>
                                    <td className="px-6 py-4 text-xs font-mono text-slate-500 align-top">{res.id || '-'}</td>
                                    <td className="px-6 py-4 text-xs font-bold text-slate-700 align-top">{formatValue(res.value)}</td>
                                    <td className="px-6 py-4 text-xs text-slate-600 leading-relaxed align-top">
                                      <div className="space-y-3">
                                        <div>
                                          <span className="text-[9px] font-black text-rose-500 uppercase tracking-widest block mb-1">Indício</span>
                                          {res.detail}
                                        </div>
                                        <div>
                                          <span className="text-[9px] font-black text-emerald-600 uppercase tracking-widest block mb-1">Sugestão de Resolução</span>
                                          <p className="text-slate-800 font-medium">{res.resolution}</p>
                                        </div>
                                        <details className="cursor-pointer group">
                                          <summary className="text-[9px] font-black text-slate-400 uppercase tracking-widest hover:text-indigo-600 transition-colors">Ver Dados Completos do Item</summary>
                                          <div className="mt-2 p-3 bg-slate-900 rounded-lg text-[10px] font-mono text-emerald-400 overflow-x-auto whitespace-pre-wrap">
                                            {res.fullData}
                                          </div>
                                        </details>
                                      </div>
                                    </td>
                                  </motion.tr>
                                ))}
                              </AnimatePresence>
                            </React.Fragment>
                          )) : (
                            <tr>
                              <td colSpan={4} className="px-6 py-12 text-center text-slate-400 text-sm italic">
                                Nenhum indício encontrado para os critérios selecionados.
                              </td>
                            </tr>
                          )}
                        </tbody>
                      </table>
                    </div>
                  </div>
                </motion.div>
              )}
            </AnimatePresence>
          </div>

        </div>
      </main>

      <footer className="bg-slate-900 text-slate-400 py-12 mt-12 border-t border-slate-800">
        <div className="max-w-7xl mx-auto px-6">
          <div className="flex flex-col md:flex-row justify-between items-center gap-8">
            <div className="flex flex-col items-center md:items-start gap-2">
              <div className="flex items-center gap-2">
                <ShieldCheck className="w-5 h-5 text-emerald-500" />
                <span className="text-white font-black tracking-widest text-sm uppercase">AuditFlow <span className="text-emerald-500">AI</span></span>
              </div>
              <p className="text-[10px] font-medium max-w-xs text-center md:text-left">
                Plataforma avançada de auditoria preditiva e análise de conformidade para o setor público.
              </p>
            </div>
            
            <div className="flex items-center gap-8">
              <div className="text-center md:text-right">
                <p className="text-[10px] font-black uppercase tracking-widest text-slate-500 mb-1">Versão</p>
                <p className="text-xs font-bold text-slate-300">2.4.0-stable</p>
              </div>
              <div className="text-center md:text-right">
                <p className="text-[10px] font-black uppercase tracking-widest text-slate-500 mb-1">Suporte</p>
                <p className="text-xs font-bold text-slate-300">suporte@auditflow.gov</p>
              </div>
            </div>
          </div>
          
          <div className="mt-12 pt-8 border-t border-slate-800/50 flex flex-col md:flex-row justify-between items-center gap-4">
            <p className="text-[10px] font-medium">
              © 2026 AuditFlow AI. Todos os direitos reservados.
            </p>
            <div className="flex items-center gap-6 text-[10px] font-bold uppercase tracking-widest">
              <a href="#" className="hover:text-white transition-colors">Privacidade</a>
              <a href="#" className="hover:text-white transition-colors">Termos de Uso</a>
              <a href="#" className="hover:text-white transition-colors">Documentação</a>
            </div>
          </div>
        </div>
      </footer>
      {/* Trails List Modal */}
      <AnimatePresence>
        {showTrailsList && (
          <div className="fixed inset-0 z-50 flex items-center justify-center p-4 bg-slate-900/60 backdrop-blur-sm">
            <motion.div 
              initial={{ scale: 0.9, opacity: 0 }}
              animate={{ scale: 1, opacity: 1 }}
              exit={{ scale: 0.9, opacity: 0 }}
              className="bg-white rounded-3xl shadow-2xl w-full max-w-2xl max-h-[80vh] overflow-hidden flex flex-col"
            >
              <div className="p-6 border-b border-slate-100 flex items-center justify-between bg-indigo-600 text-white">
                <div className="flex items-center gap-3">
                  <ShieldCheck className="w-6 h-6" />
                  <h2 className="font-bold text-lg">Trilhas de Auditoria Ativas (52)</h2>
                </div>
                <button 
                  onClick={() => setShowTrailsList(false)}
                  className="p-2 hover:bg-white/20 rounded-full transition-colors"
                >
                  <X className="w-6 h-6" />
                </button>
              </div>
              <div className="p-6 overflow-y-auto">
                <p className="text-sm text-slate-500 mb-6 font-medium">
                  Estas são as regras de negócio automatizadas que o sistema analisa em cada processamento de dados do SICOM.
                </p>
                <div className="grid grid-cols-1 md:grid-cols-2 gap-3">
                  {auditTrails.map((trail, idx) => (
                    <div key={idx} className="flex items-center gap-3 p-3 bg-slate-50 rounded-xl border border-slate-100 hover:border-indigo-200 transition-colors group">
                      <span className="text-[10px] font-black text-slate-300 group-hover:text-indigo-400 transition-colors">{(idx + 1).toString().padStart(2, '0')}</span>
                      <span className="text-xs font-bold text-slate-700">{trail}</span>
                    </div>
                  ))}
                </div>
              </div>
              <div className="p-6 border-t border-slate-100 bg-slate-50 flex justify-end">
                <button 
                  onClick={() => setShowTrailsList(false)}
                  className="px-6 py-2 bg-indigo-600 text-white rounded-xl text-xs font-bold uppercase tracking-widest hover:bg-indigo-700 transition-colors shadow-lg shadow-indigo-200"
                >
                  Fechar
                </button>
              </div>
            </motion.div>
          </div>
        )}
      </AnimatePresence>
    </div>
  );
}
